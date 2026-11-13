//! HTTP layer: request parsing, the idempotent transaction envelope shared by
//! every mutating endpoint, and the read-only wallet endpoint. Every `/v1`
//! route runs behind [`auth::require_api_key`]; all data access is scoped to
//! the caller's account.

use std::sync::Arc;

use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::HeaderMap,
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::{
    AppState,
    auth::{self, AuthContext},
    error::ApiError,
    idempotency, ledger,
    models::{CommitRequest, GrantRequest, ReleaseRequest, ReserveRequest, validate_tenant_id},
};

const RECENT_LEDGER_LIMIT: i64 = 20;

pub fn router(state: AppState) -> Router {
    let v1 = Router::new()
        .route("/v1/credits/grant", post(grant))
        .route("/v1/credits/reserve", post(reserve))
        .route("/v1/credits/commit", post(commit))
        .route("/v1/credits/release", post(release))
        .route("/v1/tenants/{tenant_id}/wallet", get(get_wallet))
        .route_layer(middleware::from_fn_with_state(state.clone(), auth::require_api_key));
    Router::new()
        .merge(v1)
        .route("/healthz", get(health))
        .with_state(state)
}

type Caller = Extension<Arc<AuthContext>>;

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Endpoint {
    Grant,
    Reserve,
    Commit,
    Release,
}

impl Endpoint {
    fn name(self) -> &'static str {
        match self {
            Self::Grant => "grant",
            Self::Reserve => "reserve",
            Self::Commit => "commit",
            Self::Release => "release",
        }
    }

    fn path(self) -> &'static str {
        match self {
            Self::Grant => "/v1/credits/grant",
            Self::Reserve => "/v1/credits/reserve",
            Self::Commit => "/v1/credits/commit",
            Self::Release => "/v1/credits/release",
        }
    }
}

enum Mutation {
    Grant(GrantRequest),
    Reserve(ReserveRequest),
    Commit(CommitRequest),
    Release(ReleaseRequest),
}

fn parse_body<T: DeserializeOwned>(value: Value) -> Result<T, ApiError> {
    serde_json::from_value(value).map_err(|e| ApiError::bad_request("invalid_request", e.to_string()))
}

impl Mutation {
    fn parse(endpoint: Endpoint, value: Value) -> Result<Self, ApiError> {
        let mutation = match endpoint {
            Endpoint::Grant => Self::Grant(parse_body(value)?),
            Endpoint::Reserve => Self::Reserve(parse_body(value)?),
            Endpoint::Commit => Self::Commit(parse_body(value)?),
            Endpoint::Release => Self::Release(parse_body(value)?),
        };
        match &mutation {
            Self::Grant(r) => r.validate()?,
            Self::Reserve(r) => r.validate()?,
            Self::Commit(r) => r.validate()?,
            Self::Release(r) => r.validate()?,
        }
        Ok(mutation)
    }

    fn tenant_id(&self) -> &str {
        match self {
            Self::Grant(r) => &r.tenant_id,
            Self::Reserve(r) => &r.tenant_id,
            Self::Commit(r) => &r.tenant_id,
            Self::Release(r) => &r.tenant_id,
        }
    }
}

/// The envelope every mutating endpoint runs in:
///
/// 1. Validate `Idempotency-Key` and the body (failures are not cached).
/// 2. Fast path: replay a stored response without taking any lock.
/// 3. `BEGIN`, lock the wallet row `FOR UPDATE` (grant creates it if needed,
///    subject to the account's tenant limit), and expire any credit bucket
///    that is past its expiry.
/// 4. Decide the outcome and its writes in memory (commit/release read the
///    reservation first; spending from a wallet with expiring credits reads
///    its buckets).
/// 5. Flush all writes plus the idempotency record in one statement, gated
///    on the idempotency insert: if a duplicate with the same key committed
///    while we waited for the lock, nothing is written and its stored
///    response is replayed instead. Otherwise `COMMIT`.
async fn execute(
    state: &AppState,
    caller: &AuthContext,
    headers: &HeaderMap,
    body: &[u8],
    endpoint: Endpoint,
) -> Result<Response, ApiError> {
    let key = idempotency::extract_key(headers)?;
    let value: Value = serde_json::from_slice(body)
        .map_err(|e| ApiError::bad_request("invalid_json", format!("request body is not valid JSON: {e}")))?;
    let hash = idempotency::request_hash(&value);
    let mutation = Mutation::parse(endpoint, value)?;
    let account_id = caller.account_id.as_str();
    let tenant_id = mutation.tenant_id().to_owned();

    if let Some(stored) = idempotency::lookup(&state.pool, account_id, &tenant_id, &key).await? {
        return idempotency::replay(stored, endpoint.path(), &hash);
    }

    let mut tx = state.pool.begin().await?;
    let locked = match &mutation {
        Mutation::Grant(_) => Some(
            ledger::lock_or_create_wallet(&mut tx, account_id, &tenant_id, caller.max_tenants).await?,
        ),
        _ => ledger::lock_wallet(&mut tx, account_id, &tenant_id).await?,
    };
    let Some(locked) = locked else {
        tx.rollback().await?;
        return Err(ApiError::wallet_not_found(&tenant_id));
    };

    let mut op = ledger::Op::begin(&mut tx, locked, endpoint.name()).await?;
    let outcome = match mutation {
        Mutation::Grant(req) => ledger::grant(&mut op, req)?,
        Mutation::Reserve(req) => ledger::reserve(&mut tx, &mut op, req).await?,
        Mutation::Commit(req) => ledger::commit(&mut tx, &mut op, req).await?,
        Mutation::Release(req) => ledger::release(&mut tx, &mut op, req).await?,
    };

    op.record_idempotency(&key, endpoint.path(), &hash, &outcome);
    if !op.flush(&mut tx).await? {
        tx.rollback().await?;
        let stored = idempotency::lookup(&state.pool, account_id, &tenant_id, &key)
            .await?
            .ok_or_else(|| ApiError::internal("idempotency key conflicted but no stored response was found"))?;
        return idempotency::replay(stored, endpoint.path(), &hash);
    }
    tx.commit().await?;

    Ok((outcome.status, Json(outcome.body)).into_response())
}

async fn grant(
    State(state): State<AppState>,
    Extension(caller): Caller,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    execute(&state, &caller, &headers, &body, Endpoint::Grant).await
}

async fn reserve(
    State(state): State<AppState>,
    Extension(caller): Caller,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    execute(&state, &caller, &headers, &body, Endpoint::Reserve).await
}

async fn commit(
    State(state): State<AppState>,
    Extension(caller): Caller,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    execute(&state, &caller, &headers, &body, Endpoint::Commit).await
}

async fn release(
    State(state): State<AppState>,
    Extension(caller): Caller,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    execute(&state, &caller, &headers, &body, Endpoint::Release).await
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

async fn get_wallet(
    State(state): State<AppState>,
    Extension(caller): Caller,
    Path(tenant_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    validate_tenant_id(&tenant_id)?;
    // One REPEATABLE READ snapshot so balances and the ledger tail agree.
    let mut tx = state.pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let Some(wallet) = ledger::fetch_wallet(&mut tx, &caller.account_id, &tenant_id).await? else {
        tx.rollback().await?;
        return Err(ApiError::wallet_not_found(&tenant_id));
    };
    let buckets = if wallet.expiring_credits > 0 {
        ledger::live_buckets(&mut tx, &caller.account_id, &tenant_id).await?
    } else {
        Vec::new()
    };
    let recent = ledger::recent_ledger(&mut tx, &caller.account_id, &tenant_id, RECENT_LEDGER_LIMIT).await?;
    tx.commit().await?;

    // Credits past their expiry are left out even if no request or reaper
    // pass has written their CREDIT_EXPIRE entry yet.
    let due: i64 = buckets.iter().filter(|b| b.due).map(|b| b.bucket.remaining).sum();
    let live: Vec<_> = buckets.into_iter().filter(|b| !b.due).map(|b| b.bucket).collect();
    Ok(Json(json!({
        "tenant_id": wallet.tenant_id,
        "available_credits": wallet.available_credits - due,
        "reserved_credits": wallet.reserved_credits,
        "expiring_credits": wallet.expiring_credits - due,
        "permanent_credits": wallet.permanent_credits(),
        "next_expiry_at": live.first().map(|b| b.expires_at),
        "credit_buckets": live,
        "updated_at": wallet.updated_at,
        "recent_ledger": recent,
    })))
}

async fn health(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    sqlx::query("SELECT 1").execute(&state.pool).await?;
    Ok(Json(json!({ "status": "ok" })))
}
