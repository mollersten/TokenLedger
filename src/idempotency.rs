//! `Idempotency-Key` handling.
//!
//! Keys are scoped per account and tenant
//! (`PRIMARY KEY (account_id, tenant_id, idempotency_key)`).
//! A key replays the stored status + body only when it is reused on the same
//! endpoint with a byte-for-byte equivalent (canonicalised) JSON body;
//! anything else is rejected with `422` instead of silently returning a
//! response that belongs to a different request.
//!
//! The response row is inserted by the same statement that applies the
//! balance change (`ledger::Op::flush`), while the tenant's wallet row
//! is locked. That insert uses `ON CONFLICT DO NOTHING` and gates every other
//! write, so concurrent requests carrying the same key serialise on the lock,
//! the first one executes, and the rest write nothing and replay its response.

use axum::{
    Json,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::error::ApiError;

pub const HEADER: &str = "idempotency-key";
pub const REPLAYED_HEADER: &str = "idempotent-replayed";
const MAX_KEY_LEN: usize = 255;

#[derive(Debug, sqlx::FromRow)]
pub struct StoredResponse {
    pub endpoint: String,
    pub request_hash: String,
    pub response_status: i16,
    pub response_body: Value,
}

pub fn extract_key(headers: &HeaderMap) -> Result<String, ApiError> {
    let raw = headers.get(HEADER).ok_or_else(|| {
        ApiError::bad_request(
            "missing_idempotency_key",
            "the Idempotency-Key header is required for this endpoint",
        )
    })?;
    let key = raw
        .to_str()
        .ok()
        .map(str::trim)
        .filter(|k| !k.is_empty() && k.len() <= MAX_KEY_LEN && k.chars().all(|c| c.is_ascii_graphic()))
        .ok_or_else(|| {
            ApiError::bad_request(
                "invalid_idempotency_key",
                format!("Idempotency-Key must be 1-{MAX_KEY_LEN} visible ASCII characters"),
            )
        })?;
    Ok(key.to_owned())
}

/// SHA-256 over the canonical JSON form (object keys sorted recursively).
pub fn request_hash(body: &Value) -> String {
    let mut canonical = String::new();
    write_canonical(body, &mut canonical);
    hex::encode(Sha256::digest(canonical.as_bytes()))
}

fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(key.clone()).to_string());
                out.push(':');
                write_canonical(&map[key], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

pub async fn lookup<'e, E>(
    executor: E,
    account_id: &str,
    tenant_id: &str,
    key: &str,
) -> Result<Option<StoredResponse>, sqlx::Error>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query_as::<_, StoredResponse>(
        "SELECT endpoint, request_hash, response_status, response_body FROM idempotency_keys \
         WHERE account_id = $1 AND tenant_id = $2 AND idempotency_key = $3",
    )
    .bind(account_id)
    .bind(tenant_id)
    .bind(key)
    .fetch_optional(executor)
    .await
}

/// Turn a stored response into the HTTP reply, or reject a mismatched reuse.
pub fn replay(stored: StoredResponse, endpoint: &str, hash: &str) -> Result<Response, ApiError> {
    check_reuse(&stored, endpoint, hash)?;
    let status = u16::try_from(stored.response_status)
        .ok()
        .and_then(|s| StatusCode::from_u16(s).ok())
        .ok_or_else(|| ApiError::internal("stored idempotency response has an invalid status"))?;
    let mut response = (status, Json(stored.response_body)).into_response();
    response
        .headers_mut()
        .insert(REPLAYED_HEADER, HeaderValue::from_static("true"));
    Ok(response)
}

/// The stored body, for results replayed inside a larger response (one
/// tenant of an import), after the same reuse checks as [`replay`].
pub fn replay_body(stored: StoredResponse, endpoint: &str, hash: &str) -> Result<Value, ApiError> {
    check_reuse(&stored, endpoint, hash)?;
    Ok(stored.response_body)
}

fn check_reuse(stored: &StoredResponse, endpoint: &str, hash: &str) -> Result<(), ApiError> {
    if stored.endpoint != endpoint {
        return Err(ApiError::unprocessable(
            "idempotency_key_reused",
            format!(
                "this Idempotency-Key was already used for {} and cannot be reused for {endpoint}",
                stored.endpoint
            ),
        ));
    }
    if stored.request_hash != hash {
        return Err(ApiError::unprocessable(
            "idempotency_key_mismatch",
            "this Idempotency-Key was already used with a different request body",
        ));
    }
    Ok(())
}

/// How long idempotency records are kept by default. A retry of the same
/// request within this window replays its answer; after it, the request is
/// decided afresh. Charging once per item does not depend on these records
/// (`wallets_items` does), so this never lets an item be charged twice.
pub const DEFAULT_RETENTION: std::time::Duration = std::time::Duration::from_secs(24 * 3600);
/// How often the server prunes them.
pub const PRUNE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(600);
const PRUNE_BATCH: i64 = 5_000;

/// Delete records older than `retention`, in batches so no statement holds
/// many row locks or runs long. Returns how many were deleted.
pub async fn prune(pool: &sqlx::PgPool, retention: std::time::Duration) -> Result<u64, sqlx::Error> {
    let mut deleted = 0;
    loop {
        let n = sqlx::query(
            "DELETE FROM idempotency_keys WHERE ctid = ANY (ARRAY( \
               SELECT ctid FROM idempotency_keys \
               WHERE created_at < NOW() - make_interval(secs => $1) LIMIT $2))",
        )
        .bind(retention.as_secs_f64())
        .bind(PRUNE_BATCH)
        .execute(pool)
        .await?
        .rows_affected();
        deleted += n;
        if n < PRUNE_BATCH as u64 {
            return Ok(deleted);
        }
    }
}

/// Every `interval`: prune idempotency records past `retention` (if set)
/// and owned items past their account's `item_retention_days`.
pub fn spawn_pruner(
    pool: sqlx::PgPool,
    retention: Option<std::time::Duration>,
    interval: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Some(retention) = retention {
                match prune(&pool, retention).await {
                    Ok(0) => {}
                    Ok(n) => tracing::info!(deleted = n, "pruned old idempotency records"),
                    Err(err) => tracing::warn!(error = %err, "pruning idempotency records failed; will retry"),
                }
            }
            match crate::ledger::prune_expired_items(&pool).await {
                Ok(0) => {}
                Ok(n) => tracing::info!(deleted = n, "pruned expired owned items"),
                Err(err) => tracing::warn!(error = %err, "pruning expired owned items failed; will retry"),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hash_ignores_key_order() {
        let a = json!({"tenant_id": "t", "amount": 5, "metadata": {"b": 1, "a": [1, {"y": 2, "x": 1}]}});
        let b = json!({"metadata": {"a": [1, {"x": 1, "y": 2}], "b": 1}, "amount": 5, "tenant_id": "t"});
        assert_eq!(request_hash(&a), request_hash(&b));
    }

    #[test]
    fn hash_detects_value_changes() {
        assert_ne!(request_hash(&json!({"amount": 5})), request_hash(&json!({"amount": 6})));
    }
}
