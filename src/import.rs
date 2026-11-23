//! `POST /v1/import`: moving customers over from another billing system.
//!
//! A request carries up to [`MAX_IMPORT_TENANTS`] tenants. Each one is
//! imported in its own transaction, under its wallet lock like every other
//! write, so one failing tenant does not undo the others and no request
//! holds more than one wallet lock at a time. Imports do not count toward
//! the account's limits (see [`crate::auth`]); the per-request caps here
//! only keep each request short, and the SDK splits larger imports.
//!
//! Per tenant, both parts are optional:
//! - `credits`: the starting balance, written as grants (expiring ones keep
//!   their expiry). A tenant's balance can be imported **once**
//!   (`wallet_imports.credits_imported_at`); later changes go through `grant`, which
//!   counts toward the limits, so free imports cannot replace it.
//! - `purchases`: items bought in the old system. Each becomes a row in
//!   `wallets_items` (creating the item in `items` if it is new), so
//!   `chargeOnce` treats it as owned. They move no credits and write no
//!   reservations or ledger entries. Items the tenant has already paid for are
//!   skipped, so purchases can also be imported later; while one of them is
//!   being charged the tenant is refused (`item_charge_in_progress`) until it
//!   settles.
//!
//! Imports don't count toward the request limits (per second, per hour, per
//! month): moving over as many customers as an account likes is the point.
//! The account's tenant limit (`max_tenants`) still applies, as every wallet
//! counts toward it. A tenant that has never been seen can
//! therefore get its first balance through an import rather than `grant`;
//! the "once per tenant" rule only stops repeat imports into the same tenant.
//! What bounds an account is [`ImportCaps`] (per-day safety ceilings far
//! above any real migration) and at most [`crate::gate::IMPORT_PERMITS`]
//! import requests running at once.
//!
//! The Idempotency-Key names the import and is stored per tenant: sending
//! the same tenant again with the same key replays its result, with a
//! different body it is refused, so rerunning a migration never imports
//! anything twice. Tenants that failed are not stored and can be retried.

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde_json::{Map, Value, json};

use crate::{
    AppState,
    auth::AuthContext,
    error::ApiError,
    idempotency,
    ledger::{self, Op},
    models::{GrantRequest, ImportTenant, MAX_IMPORT_PURCHASES, MAX_IMPORT_TENANTS, Outcome},
};

pub const ENDPOINT: &str = "/v1/import";

pub const DEFAULT_CUSTOMERS_PER_DAY: i64 = 1_000_000;
pub const DEFAULT_PURCHASES_PER_DAY: i64 = 50_000_000;

/// Safety ceilings per account and UTC day (`IMPORT_CUSTOMERS_PER_DAY`,
/// `IMPORT_PURCHASES_PER_DAY`). Not plan limits: they are far above any
/// real migration and only stop runaway or malicious clients. A customer
/// counts once, the first time it is imported (replays, retries and later
/// purchase imports for it don't count again); purchases count when rows are
/// actually written (items already paid for don't).
#[derive(Debug, Clone, Copy)]
pub struct ImportCaps {
    pub customers_per_day: i64,
    pub purchases_per_day: i64,
}

impl Default for ImportCaps {
    fn default() -> Self {
        Self { customers_per_day: DEFAULT_CUSTOMERS_PER_DAY, purchases_per_day: DEFAULT_PURCHASES_PER_DAY }
    }
}

const TODAY: &str = "(NOW() AT TIME ZONE 'UTC')::date";

fn seconds_until_tomorrow() -> u64 {
    let now = Utc::now();
    let tomorrow = (now.date_naive() + chrono::Days::new(1)).and_hms_opt(0, 0, 0).expect("midnight").and_utc();
    (tomorrow - now).num_seconds().max(1) as u64
}

fn limit_reached(caps: &ImportCaps) -> ApiError {
    ApiError::too_many_requests(
        "import_limit_reached",
        format!(
            "this account has reached today's import limit ({} customers and {} purchases per UTC day); \
             rerun the same import after midnight UTC to carry on",
            caps.customers_per_day, caps.purchases_per_day
        ),
        Some(seconds_until_tomorrow()),
    )
}

/// Count `customers` and `purchases` against today's caps, or refuse if
/// they would go over. Runs in the tenant's transaction, so a tenant that
/// fails later gives its share back. The row lock on the account's counter
/// serialises concurrent imports of one account.
async fn take_budget(
    conn: &mut sqlx::PgConnection,
    account_id: &str,
    customers: i64,
    purchases: i64,
    caps: &ImportCaps,
) -> Result<(), ApiError> {
    if customers == 0 && purchases == 0 {
        return Ok(());
    }
    if customers > caps.customers_per_day || purchases > caps.purchases_per_day {
        return Err(limit_reached(caps));
    }
    let taken: Option<i32> = sqlx::query_scalar(&format!(
        "INSERT INTO account_import_usage (account_id, day, customers, purchases) VALUES ($1, {TODAY}, $2, $3) \
         ON CONFLICT (account_id, day) DO UPDATE SET \
           customers = account_import_usage.customers + EXCLUDED.customers, \
           purchases = account_import_usage.purchases + EXCLUDED.purchases \
         WHERE account_import_usage.customers + EXCLUDED.customers <= $4 \
           AND account_import_usage.purchases + EXCLUDED.purchases <= $5 \
         RETURNING 1"
    ))
    .bind(account_id)
    .bind(customers)
    .bind(purchases)
    .bind(caps.customers_per_day)
    .bind(caps.purchases_per_day)
    .fetch_optional(&mut *conn)
    .await?;
    taken.map(|_| ()).ok_or_else(|| limit_reached(caps))
}

/// Refuse the whole request up front once both of today's caps are used up,
/// so a client that keeps sending costs one cheap query per request.
pub async fn check_caps(state: &AppState, account_id: &str) -> Result<(), ApiError> {
    let used: Option<(i64, i64)> = sqlx::query_as(&format!(
        "SELECT customers, purchases FROM account_import_usage WHERE account_id = $1 AND day = {TODAY}"
    ))
    .bind(account_id)
    .fetch_optional(&state.pool)
    .await?;
    let caps = &state.import_caps;
    match used {
        Some((c, p)) if c >= caps.customers_per_day && p >= caps.purchases_per_day => Err(limit_reached(caps)),
        _ => Ok(()),
    }
}

/// A tenant entry that passed validation, or why it did not.
pub type Entry = Result<(Value, ImportTenant), (Value, ApiError)>;

/// Check the request's shape and limits (a 400 for the whole request if they
/// are wrong), then validate each tenant on its own: an invalid tenant is
/// reported as failed and does not stop the others.
pub fn parse(body: Value) -> Result<Vec<Entry>, ApiError> {
    let invalid = |message: String| ApiError::bad_request("invalid_import", message);
    let Value::Object(mut map) = body else {
        return Err(invalid("expected an object with a `tenants` array".to_owned()));
    };
    let tenants = match map.remove("tenants") {
        Some(Value::Array(tenants)) => tenants,
        _ => return Err(invalid("expected an object with a `tenants` array".to_owned())),
    };
    if let Some(field) = map.keys().next() {
        return Err(invalid(format!("unknown field `{field}`")));
    }
    if tenants.is_empty() || tenants.len() > MAX_IMPORT_TENANTS {
        return Err(invalid(format!("send 1-{MAX_IMPORT_TENANTS} tenants per request")));
    }
    let purchases: usize =
        tenants.iter().map(|t| t.get("purchases").and_then(Value::as_array).map_or(0, Vec::len)).sum();
    if purchases > MAX_IMPORT_PURCHASES {
        return Err(invalid(format!("at most {MAX_IMPORT_PURCHASES} purchases per request")));
    }
    let mut seen = std::collections::HashSet::new();
    Ok(tenants
        .into_iter()
        .map(|raw| {
            let tenant_id = raw.get("tenant_id").cloned().unwrap_or(Value::Null);
            let tenant = match serde_json::from_value::<ImportTenant>(raw.clone()) {
                Ok(tenant) => tenant,
                Err(e) => return Err((tenant_id, ApiError::bad_request("invalid_request", e.to_string()))),
            };
            if let Err(e) = tenant.validate() {
                return Err((tenant_id, e));
            }
            if !seen.insert(tenant.tenant_id.clone()) {
                return Err((tenant_id, invalid(format!("tenant '{}' appears twice", tenant.tenant_id))));
            }
            Ok((raw, tenant))
        })
        .collect())
}

/// Import every tenant, in order. Returns the response body; tenants that
/// fail are reported in it. Only an internal error fails the whole request
/// (tenants done before it are kept, and a retry with the same key replays
/// them and carries on).
pub async fn run(state: &AppState, caller: &AuthContext, key: &str, tenants: Vec<Entry>) -> Result<Value, ApiError> {
    let (mut imported, mut replayed, mut failed) = (0, 0, 0);
    let mut results = Vec::with_capacity(tenants.len());
    for entry in tenants {
        let (raw, tenant) = match entry {
            Ok(parsed) => parsed,
            Err((tenant_id, error)) => {
                failed += 1;
                let (code, message) = error.client_parts();
                results.push(json!({ "tenant_id": tenant_id, "status": "failed", "error": code, "message": message }));
                continue;
            }
        };
        let tenant_id = tenant.tenant_id.clone();
        match import_tenant(state, caller, key, &raw, tenant).await {
            Ok((mut body, was_replayed)) => {
                if was_replayed {
                    replayed += 1;
                } else {
                    imported += 1;
                }
                body["status"] = json!(if was_replayed { "replayed" } else { "imported" });
                results.push(body);
            }
            Err(ApiError::Internal(detail)) => return Err(ApiError::Internal(detail)),
            Err(error) => {
                failed += 1;
                let (code, message) = error.client_parts();
                results.push(json!({ "tenant_id": tenant_id, "status": "failed", "error": code, "message": message }));
            }
        }
    }
    Ok(json!({ "imported": imported, "replayed": replayed, "failed": failed, "results": results }))
}

/// Import one tenant. Returns its result and whether it was replayed.
async fn import_tenant(
    state: &AppState,
    caller: &AuthContext,
    key: &str,
    raw: &Value,
    tenant: ImportTenant,
) -> Result<(Value, bool), ApiError> {
    let account_id = caller.account_id.as_str();
    let tenant_id = tenant.tenant_id.clone();
    let hash = idempotency::request_hash(raw);
    if let Some(stored) = idempotency::lookup(&state.pool, account_id, &tenant_id, key).await? {
        return Ok((idempotency::replay_body(stored, ENDPOINT, &hash)?, true));
    }

    let _permit = state.gates.wallet(account_id, &tenant_id).await?;
    let mut tx = state.pool.begin().await?;
    let locked = ledger::lock_or_create_wallet(&mut tx, account_id, &tenant_id, caller.max_tenants).await?;
    // The same import may have finished this tenant while we waited for the lock.
    if let Some(stored) = idempotency::lookup(&mut *tx, account_id, &tenant_id, key).await? {
        tx.rollback().await?;
        return Ok((idempotency::replay_body(stored, ENDPOINT, &hash)?, true));
    }
    let mut op = Op::begin(&mut tx, locked, "import").await?;
    let (credits_imported_at, credits_import_key, first_imported_at): (
        Option<DateTime<Utc>>,
        Option<String>,
        Option<DateTime<Utc>>,
    ) = sqlx::query_as::<_, (Option<DateTime<Utc>>, Option<String>, Option<DateTime<Utc>>)>(
        "SELECT credits_imported_at, credits_import_key, first_imported_at FROM wallet_imports WHERE wallet_id = $1",
    )
    .bind(op.wallet.id)
    .fetch_optional(&mut *tx)
    .await?
    .unwrap_or((None, None, None));
    // The balance can come in once. The same import run again (after its
    // idempotency records were pruned) skips it; any other import is refused.
    let mut credits = tenant.credits;
    if !credits.is_empty()
        && let Some(at) = credits_imported_at
    {
        if credits_import_key.as_deref() != Some(key) {
            return Err(ApiError::conflict(
                "credits_already_imported",
                format!("tenant '{tenant_id}' had its balance imported at {at}; use grant for later changes"),
                None,
            ));
        }
        credits.clear();
    }

    // Decide which purchases are new before writing anything, so the
    // budget check below counts exactly the rows that will be written.
    let now = op.now;
    let wallet_id = op.wallet.id;
    let mut skipped: Vec<String> = Vec::new();
    // (item id, credits paid, purchased at) for each wallets_items row to write.
    let mut new_purchases: Vec<(i64, i64, DateTime<Utc>)> = Vec::new();
    if !tenant.purchases.is_empty() {
        if let Some(p) = tenant.purchases.iter().find(|p| p.purchased_at.is_some_and(|at| at > now)) {
            return Err(ApiError::unprocessable(
                "invalid_purchased_at",
                format!("purchase '{}' has purchased_at in the future", p.item_key),
            ));
        }
        // Items belong to the account and are shared by its tenants: create
        // the new ones (sorted, so concurrent imports take the unique-index
        // locks in the same order and cannot deadlock), then read all the ids.
        let mut pairs: Vec<(&str, &str)> =
            tenant.purchases.iter().map(|p| (p.feature.as_str(), p.item_key.as_str())).collect();
        pairs.sort_unstable();
        let (features, keys): (Vec<&str>, Vec<&str>) = pairs.iter().copied().unzip();
        sqlx::query(
            "INSERT INTO items (account_id, feature, item_key) \
             SELECT $1, t.f, t.k FROM UNNEST($2::text[], $3::text[]) AS t(f, k) \
             ON CONFLICT (account_id, feature, item_key) DO NOTHING",
        )
        .bind(account_id)
        .bind(&features)
        .bind(&keys)
        .execute(&mut *tx)
        .await?;
        let found: Vec<(i64, String, String, bool, bool)> = sqlx::query_as(
            "SELECT i.id, i.feature, i.item_key, \
                    EXISTS (SELECT 1 FROM wallets_items wi JOIN accounts a ON a.id = $1 \
                              WHERE wi.wallet_id = $4 AND wi.item_id = i.id \
                                AND (a.item_retention_days IS NULL \
                                     OR wi.created_at > NOW() - make_interval(days => a.item_retention_days))), \
                    EXISTS (SELECT 1 FROM reservations r WHERE r.account_id = $1 AND r.tenant_id = $5 \
                              AND r.item_id = i.id AND r.status = 'PENDING') \
             FROM items i JOIN UNNEST($2::text[], $3::text[]) AS t(f, k) ON i.feature = t.f AND i.item_key = t.k \
             WHERE i.account_id = $1",
        )
        .bind(account_id)
        .bind(&features)
        .bind(&keys)
        .bind(wallet_id)
        .bind(&tenant_id)
        .fetch_all(&mut *tx)
        .await?;
        // An item being charged right now may still be released, which would
        // leave it unpaid although it was bought in the old system. Refuse the
        // tenant for now (not stored, so the same import can be retried).
        let pending: Vec<&str> = found.iter().filter(|r| r.4).map(|r| r.2.as_str()).take(10).collect();
        if !pending.is_empty() {
            return Err(ApiError::conflict(
                "item_charge_in_progress",
                format!("items being charged right now: {}; retry the import once they settle", pending.join(", ")),
                None,
            ));
        }
        let items: std::collections::HashMap<(&str, &str), (i64, bool)> =
            found.iter().map(|(id, f, k, owned, _)| ((f.as_str(), k.as_str()), (*id, *owned))).collect();
        for p in &tenant.purchases {
            let (item_id, owned) = *items
                .get(&(p.feature.as_str(), p.item_key.as_str()))
                .ok_or_else(|| ApiError::internal("an imported item was not found after inserting it"))?;
            if owned {
                // Already owned (bought here, or imported before): keep it.
                skipped.push(p.item_key.clone());
            } else {
                new_purchases.push((item_id, p.credits, p.purchased_at.unwrap_or(now)));
            }
        }
    }

    let moved = i64::from(first_imported_at.is_none());
    take_budget(&mut tx, account_id, moved, new_purchases.len() as i64, &state.import_caps).await?;
    sqlx::query(
        "INSERT INTO wallet_imports (wallet_id, credits_imported_at, credits_import_key) \
         VALUES ($1, CASE WHEN $2 THEN NOW() END, CASE WHEN $2 THEN $3 END) \
         ON CONFLICT (wallet_id) DO UPDATE SET \
           credits_imported_at = COALESCE(EXCLUDED.credits_imported_at, wallet_imports.credits_imported_at), \
           credits_import_key = COALESCE(EXCLUDED.credits_import_key, wallet_imports.credits_import_key)",
    )
    .bind(op.wallet.id)
    .bind(!credits.is_empty())
    .bind(key)
    .execute(&mut *tx)
    .await?;

    let mut granted: i64 = 0;
    if !credits.is_empty() {
        for credit in credits {
            let mut metadata = Map::new();
            metadata.insert("import".to_owned(), json!(key));
            let outcome = ledger::grant(
                &mut op,
                GrantRequest {
                    tenant_id: tenant_id.clone(),
                    amount: credit.amount,
                    reason: Some(credit.reason.unwrap_or_else(|| "import".to_owned())),
                    metadata: Some(metadata),
                    expires_at: credit.expires_at,
                },
            )?;
            if outcome.status != StatusCode::OK {
                return Err(ApiError::unprocessable("balance_overflow", "the imported credits overflow the wallet balance"));
            }
            granted += credit.amount;
            // A write set holds one new bucket, so write each grant as it comes.
            if !op.flush(&mut tx).await? {
                return Err(ApiError::internal("import grant was gated without an idempotency record"));
            }
        }
    }

    let mut purchases_imported = 0;
    if !new_purchases.is_empty() {
        purchases_imported = sqlx::query(
            "INSERT INTO wallets_items (wallet_id, item_id, credits_paid, created_at) \
             SELECT $1, t.item, t.credits, t.at FROM UNNEST($2::int8[], $3::int8[], $4::timestamptz[]) \
               AS t(item, credits, at) \
             ON CONFLICT (wallet_id, item_id) DO UPDATE SET credits_paid = EXCLUDED.credits_paid, \
               reservation_id = NULL, created_at = EXCLUDED.created_at \
             WHERE wallets_items.created_at < EXCLUDED.created_at",
        )
        .bind(wallet_id)
        .bind(new_purchases.iter().map(|p| p.0).collect::<Vec<_>>())
        .bind(new_purchases.iter().map(|p| p.1).collect::<Vec<_>>())
        .bind(new_purchases.iter().map(|p| p.2).collect::<Vec<_>>())
        .execute(&mut *tx)
        .await?
        .rows_affected();
    }

    let body = json!({
        "tenant_id": tenant_id,
        "granted_credits": granted,
        "purchases_imported": purchases_imported,
        "purchases_skipped": skipped.len(),
        "skipped_item_keys": skipped,
        "available_credits": op.wallet.available_credits,
        "expiring_credits": op.wallet.expiring_credits,
    });
    op.record_idempotency(key, ENDPOINT, &hash, &Outcome::ok(body.clone()));
    if !op.flush(&mut tx).await? {
        tx.rollback().await?;
        let stored = idempotency::lookup(&state.pool, account_id, &tenant_id, key)
            .await?
            .ok_or_else(|| ApiError::internal("idempotency key conflicted but no stored response was found"))?;
        return Ok((idempotency::replay_body(stored, ENDPOINT, &hash)?, true));
    }
    tx.commit().await?;
    // Nothing new written: an earlier run of this import already did it all.
    let nothing_new = granted == 0 && purchases_imported == 0;
    Ok((body, nothing_new))
}
