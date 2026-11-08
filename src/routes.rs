//! HTTP layer: request parsing, the idempotent transaction envelope shared by
//! every mutating endpoint, and the read-only wallet endpoint.

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::{
    AppState,
    error::ApiError,
    idempotency, ledger,
    models::{CommitRequest, GrantRequest, Outcome, ReleaseRequest, ReserveRequest, validate_tenant_id},
};

const RECENT_LEDGER_LIMIT: i64 = 20;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/credits/grant", post(grant))
        .route("/v1/credits/reserve", post(reserve))
        .route("/v1/credits/commit", post(commit))
        .route("/v1/credits/release", post(release))
        .route("/v1/tenants/{tenant_id}/wallet", get(get_wallet))
        .route("/healthz", get(health))
        .with_state(state)
}

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
/// 3. `BEGIN`, lock the wallet row `FOR UPDATE` (grant upserts it first).
/// 4. Re-check the key under the lock: a concurrent duplicate that committed
///    while we waited is replayed instead of executed twice.
/// 5. Run the operation, store its response in `idempotency_keys`, `COMMIT`.
async fn execute(
    state: &AppState,
    headers: &HeaderMap,
    body: &[u8],
    endpoint: Endpoint,
) -> Result<Response, ApiError> {
    let key = idempotency::extract_key(headers)?;
    let value: Value = serde_json::from_slice(body)
        .map_err(|e| ApiError::bad_request("invalid_json", format!("request body is not valid JSON: {e}")))?;
    let hash = idempotency::request_hash(&value);
    let mutation = Mutation::parse(endpoint, value)?;
    let tenant_id = mutation.tenant_id().to_owned();

    if let Some(stored) = idempotency::lookup(&state.pool, &tenant_id, &key).await? {
        return idempotency::replay(stored, endpoint.path(), &hash);
    }

    let mut tx = state.pool.begin().await?;
    if matches!(mutation, Mutation::Grant(_)) {
        ledger::ensure_wallet(&mut tx, &tenant_id).await?;
    }
    let Some(mut wallet) = ledger::lock_wallet(&mut tx, &tenant_id).await? else {
        tx.rollback().await?;
        return Err(ApiError::wallet_not_found(&tenant_id));
    };
    if let Some(stored) = idempotency::lookup(&mut *tx, &tenant_id, &key).await? {
        tx.rollback().await?;
        return idempotency::replay(stored, endpoint.path(), &hash);
    }

    let outcome: Outcome = match mutation {
        Mutation::Grant(req) => ledger::grant(&mut tx, &mut wallet, req).await?,
        Mutation::Reserve(req) => ledger::reserve(&mut tx, &mut wallet, req).await?,
        Mutation::Commit(req) => ledger::commit(&mut tx, &mut wallet, req).await?,
        Mutation::Release(req) => ledger::release(&mut tx, &mut wallet, req).await?,
    };

    idempotency::store(&mut tx, &tenant_id, &key, endpoint.path(), &hash, &outcome).await?;
    tx.commit().await?;

    Ok((outcome.status, Json(outcome.body)).into_response())
}

async fn grant(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Result<Response, ApiError> {
    execute(&state, &headers, &body, Endpoint::Grant).await
}

async fn reserve(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Result<Response, ApiError> {
    execute(&state, &headers, &body, Endpoint::Reserve).await
}

async fn commit(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Result<Response, ApiError> {
    execute(&state, &headers, &body, Endpoint::Commit).await
}

async fn release(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Result<Response, ApiError> {
    execute(&state, &headers, &body, Endpoint::Release).await
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

async fn get_wallet(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    validate_tenant_id(&tenant_id)?;
    // One REPEATABLE READ snapshot so balances and the ledger tail agree.
    let mut tx = state.pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let Some(wallet) = ledger::fetch_wallet(&mut tx, &tenant_id).await? else {
        tx.rollback().await?;
        return Err(ApiError::wallet_not_found(&tenant_id));
    };
    let recent = ledger::recent_ledger(&mut tx, &tenant_id, RECENT_LEDGER_LIMIT).await?;
    tx.commit().await?;

    Ok(Json(json!({
        "tenant_id": wallet.tenant_id,
        "available_credits": wallet.available_credits,
        "reserved_credits": wallet.reserved_credits,
        "updated_at": wallet.updated_at,
        "recent_ledger": recent,
    })))
}

async fn health(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    sqlx::query("SELECT 1").execute(&state.pool).await?;
    Ok(Json(json!({ "status": "ok" })))
}
