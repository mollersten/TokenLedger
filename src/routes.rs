//! HTTP layer: request parsing, the idempotent transaction envelope shared by
//! every mutating endpoint, and the read-only wallet endpoint. Every `/v1`
//! route runs behind [`auth::require_api_key`]; all data access is scoped to
//! the caller's account.

use std::sync::Arc;

use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::{
    AppState, accounts,
    auth::{self, AuthContext},
    error::ApiError,
    idempotency, import, ledger,
    models::{CommitRequest, GrantRequest, ReleaseRequest, ReserveRequest, validate_tenant_id},
};

const RECENT_LEDGER_LIMIT: i64 = 20;
const IMPORT_BODY_LIMIT: usize = 16 * 1024 * 1024;

pub fn router(state: AppState) -> Router {
    let v1 = Router::new()
        .route("/v1/credits/grant", post(grant))
        .route("/v1/credits/reserve", post(reserve))
        .route("/v1/credits/commit", post(commit))
        .route("/v1/credits/release", post(release))
        // 100 tenants with 5 000 purchases and long keys can exceed axum's
        // 2 MB default; this is still bounded by the counts in `import::parse`.
        .route("/v1/import", post(import_tenants).layer(DefaultBodyLimit::max(IMPORT_BODY_LIMIT)))
        .route("/v1/settings", get(get_settings).patch(update_settings))
        .route("/v1/tenants/{tenant_id}/wallet", get(get_wallet))
        .route_layer(middleware::from_fn_with_state(state.clone(), auth::require_api_key));
    Router::new()
        .merge(v1)
        // Readiness (can reach the database) vs. liveness (the process
        // answers): a database blip should not get every instance restarted.
        .route("/healthz", get(health))
        .route("/livez", get(|| async { Json(json!({ "status": "ok" })) }))
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

    // Wait here, without a database connection, while this wallet already
    // has requests in flight (see `gate`).
    let _permit = state.gates.wallet(account_id, &tenant_id).await?;
    let mut tx = state.pool.begin().await?;
    let locked = match &mutation {
        Mutation::Grant(_) => Some(
            ledger::lock_or_create_wallet(&mut tx, account_id, &tenant_id, caller.max_tenants).await?,
        ),
        _ => ledger::lock_wallet(&mut tx, account_id, &tenant_id).await?,
    };
    let Some(locked) = locked else {
        tx.rollback().await?;
        // A tenant that was never granted anything has no wallet yet: to a
        // reserve that means 0 credits, the same answer as an empty wallet
        // (not stored for replay: there is no wallet to store it under).
        if let Mutation::Reserve(req) = &mutation {
            return Ok((
                StatusCode::PAYMENT_REQUIRED,
                Json(json!({
                    "allowed": false,
                    "error": "insufficient_credits",
                    "available_credits": 0,
                    "requested_credits": req.estimate_credits,
                })),
            )
                .into_response());
        }
        return Err(ApiError::wallet_not_found(&tenant_id));
    };
    // An item reserve decides from the item's state, which a duplicate of
    // this request (same key) may have just changed while we waited for the
    // lock. Answer that duplicate's stored response rather than a conflict.
    if let Mutation::Reserve(req) = &mutation
        && req.item_key.is_some()
        && let Some(stored) = idempotency::lookup(&mut *tx, account_id, &tenant_id, &key).await?
    {
        tx.rollback().await?;
        return idempotency::replay(stored, endpoint.path(), &hash);
    }

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

/// Move up to 100 tenants over from another billing system (see [`import`]).
/// Answers 200 with a result per tenant, including the ones that failed.
async fn import_tenants(
    State(state): State<AppState>,
    Extension(caller): Caller,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let key = idempotency::extract_key(&headers)?;
    let value: Value = serde_json::from_slice(&body)
        .map_err(|e| ApiError::bad_request("invalid_json", format!("request body is not valid JSON: {e}")))?;
    let tenants = import::parse(value)?;
    // Imports are not limited in volume, only in how many run at once per
    // account, so one client cannot occupy the pool with parallel imports.
    let Some(_permit) = state.gates.imports.try_acquire(&caller.account_id) else {
        return Err(ApiError::conflict(
            "import_in_progress",
            format!("{} imports are already running for this account; send the next one when they finish", crate::gate::IMPORT_PERMITS),
            Some(1),
        ));
    };
    import::check_caps(&state, &caller.account_id).await?;
    Ok(Json(import::run(&state, &caller, &key, tenants).await?))
}

// ---------------------------------------------------------------------------
// Account settings
// ---------------------------------------------------------------------------

fn settings_body(item_retention_days: Option<i32>) -> Json<Value> {
    Json(json!({ "item_retention_days": item_retention_days }))
}

async fn get_settings(State(state): State<AppState>, Extension(caller): Caller) -> Result<Json<Value>, ApiError> {
    Ok(settings_body(accounts::item_retention_days(&state.pool, &caller.account_id).await?))
}

/// `{"item_retention_days": 90}` makes owned items expire 90 days after they
/// were bought (the next request for one is charged again); `null` keeps
/// them forever. Takes effect at once.
async fn update_settings(
    State(state): State<AppState>,
    Extension(caller): Caller,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let invalid = |message: String| ApiError::bad_request("invalid_settings", message);
    let value: Value = serde_json::from_slice(&body)
        .map_err(|e| ApiError::bad_request("invalid_json", format!("request body is not valid JSON: {e}")))?;
    let Value::Object(map) = value else {
        return Err(invalid("expected an object".to_owned()));
    };
    if let Some(field) = map.keys().find(|k| k.as_str() != "item_retention_days") {
        return Err(invalid(format!("unknown field `{field}`")));
    }
    let max = accounts::MAX_ITEM_RETENTION_DAYS;
    let days = match map.get("item_retention_days") {
        None => return Err(invalid("send item_retention_days (a number of days, or null for forever)".to_owned())),
        Some(Value::Null) => None,
        Some(v) => match v.as_i64() {
            Some(d) if (1..=i64::from(max)).contains(&d) => Some(d as i32),
            _ => return Err(invalid(format!("item_retention_days must be 1-{max} or null"))),
        },
    };
    accounts::set_item_retention_days(&state.pool, &caller.account_id, days).await?;
    Ok(settings_body(days))
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
