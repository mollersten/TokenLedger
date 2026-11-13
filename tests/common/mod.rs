//! Shared integration-test harness: builds the real router on a real
//! PostgreSQL database and drives it in-process with `tower::ServiceExt`.
//!
//! Every `TestApp` gets its own account and API key, so tests are isolated
//! from each other and repeatable against a persistent database.
//!
//! Uses `TEST_DATABASE_URL` (never `DATABASE_URL`, so tests can't hit a
//! remote database by accident), defaulting to the local dev database from
//! `scripts/local-pg.sh` / `docker-compose.yml`.
#![allow(dead_code)]

use std::time::Duration;

use chrono::{DateTime, Utc};
use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Method, Request, StatusCode, header},
};
use backend::{
    AppState,
    accounts::{self, NewAccount},
    auth, router,
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

pub const DEFAULT_TEST_DATABASE_URL: &str = "postgres://postgres:postgres@localhost:5432/tokenledger";

/// A router plus the API key to call it with. Cheap to clone into tasks.
#[derive(Clone)]
pub struct Api {
    pub router: Router,
    pub api_key: Option<String>,
}

impl Api {
    pub fn with_key(&self, key: Option<&str>) -> Self {
        Self { router: self.router.clone(), api_key: key.map(str::to_owned) }
    }
}

pub struct TestApp {
    pub api: Api,
    pub pool: PgPool,
    pub state: AppState,
    pub account_id: String,
    pub key_id: String,
}

#[derive(Debug, Clone)]
pub struct TestResponse {
    pub status: StatusCode,
    pub body: Value,
    pub headers: HeaderMap,
    pub replayed: bool,
}

pub fn unique_tenant(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

pub fn new_key() -> String {
    Uuid::new_v4().to_string()
}

pub async fn send(
    api: &Api,
    method: Method,
    path: &str,
    idempotency_key: Option<&str>,
    body: Option<Value>,
) -> TestResponse {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(key) = &api.api_key {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {key}"));
    }
    if let Some(key) = idempotency_key {
        builder = builder.header("Idempotency-Key", key);
    }
    let request = match body {
        Some(b) => builder
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(b.to_string()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let response = api.router.clone().oneshot(request).await.expect("router call failed");
    let status = response.status();
    let headers = response.headers().clone();
    let replayed = headers.contains_key("idempotent-replayed");
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
    };
    TestResponse { status, body, headers, replayed }
}

pub async fn post(api: &Api, path: &str, key: &str, body: Value) -> TestResponse {
    send(api, Method::POST, path, Some(key), Some(body)).await
}

pub async fn test_pool() -> PgPool {
    let _ = dotenvy::dotenv();
    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| DEFAULT_TEST_DATABASE_URL.to_owned());
    let pool = backend::connect(&url, 30)
        .await
        .expect("cannot reach the test database; start it with `scripts/local-pg.sh start`");
    backend::MIGRATOR.run(&pool).await.expect("migrations failed");
    pool
}

impl TestApp {
    /// An unlimited paid account with one API key.
    pub async fn spawn() -> Self {
        Self::spawn_full(NewAccount::unlimited("integration test"), auth::DEFAULT_AUTH_CACHE_TTL).await
    }

    pub async fn spawn_with(account: NewAccount) -> Self {
        Self::spawn_full(account, auth::DEFAULT_AUTH_CACHE_TTL).await
    }

    /// `auth_cache_ttl` of zero makes key/account changes visible immediately.
    pub async fn spawn_full(account: NewAccount, auth_cache_ttl: Duration) -> Self {
        let pool = test_pool().await;
        let state = AppState::with_auth_cache_ttl(pool.clone(), auth_cache_ttl);
        let account = accounts::create_account(&pool, &account).await.expect("create account");
        let key = accounts::issue_key(&pool, &account.id, Some("integration test")).await.expect("issue key");
        let router = router(state.clone());
        Self {
            api: Api { router, api_key: Some(key.secret) },
            pool,
            state,
            account_id: account.id,
            key_id: key.id,
        }
    }

    /// Another account (with its own key) served by the same router.
    pub async fn second_account(&self, account: NewAccount) -> (String, Api) {
        let account = accounts::create_account(&self.pool, &account).await.expect("create account");
        let key = accounts::issue_key(&self.pool, &account.id, None).await.expect("issue key");
        (account.id, self.api.with_key(Some(&key.secret)))
    }

    pub async fn post(&self, path: &str, key: &str, body: Value) -> TestResponse {
        post(&self.api, path, key, body).await
    }

    pub async fn get(&self, path: &str) -> TestResponse {
        send(&self.api, Method::GET, path, None, None).await
    }

    pub async fn grant_raw(&self, tenant: &str, amount: i64) -> TestResponse {
        self.post(
            "/v1/credits/grant",
            &new_key(),
            json!({ "tenant_id": tenant, "amount": amount, "reason": "test_seed" }),
        )
        .await
    }

    pub async fn grant(&self, tenant: &str, amount: i64) -> TestResponse {
        let res = self.grant_raw(tenant, amount).await;
        assert_eq!(res.status, StatusCode::OK, "grant failed: {}", res.body);
        res
    }

    /// Grant credits that expire at `expires_at`; returns the bucket id.
    pub async fn grant_expiring(&self, tenant: &str, amount: i64, expires_at: DateTime<Utc>) -> Uuid {
        let res = self
            .post(
                "/v1/credits/grant",
                &new_key(),
                json!({
                    "tenant_id": tenant,
                    "amount": amount,
                    "reason": "monthly_allowance",
                    "expires_at": expires_at.to_rfc3339(),
                }),
            )
            .await;
        assert_eq!(res.status, StatusCode::OK, "expiring grant failed: {}", res.body);
        res.body["bucket_id"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| panic!("no bucket_id in {}", res.body))
    }

    pub async fn reserve(&self, tenant: &str, estimate: i64, ttl_seconds: i64) -> TestResponse {
        self.post(
            "/v1/credits/reserve",
            &new_key(),
            json!({
                "tenant_id": tenant,
                "feature": "ai_enrichment",
                "estimate_credits": estimate,
                "ttl_seconds": ttl_seconds,
                "metadata": {},
            }),
        )
        .await
    }

    pub async fn commit(&self, tenant: &str, reservation_id: Uuid, actual: i64, key: &str) -> TestResponse {
        self.post(
            "/v1/credits/commit",
            key,
            json!({
                "tenant_id": tenant,
                "reservation_id": reservation_id,
                "actual_credits_used": actual,
                "metadata": {},
            }),
        )
        .await
    }

    pub async fn release(&self, tenant: &str, reservation_id: Uuid, key: &str) -> TestResponse {
        self.post(
            "/v1/credits/release",
            key,
            json!({ "tenant_id": tenant, "reservation_id": reservation_id, "reason": "upstream_api_failed" }),
        )
        .await
    }

    /// Lock a wallet in a separate transaction, as an in-flight request would.
    pub async fn hold_wallet_lock(&self, tenant: &str) -> sqlx::Transaction<'static, sqlx::Postgres> {
        let mut tx = self.pool.begin().await.unwrap();
        sqlx::query("SELECT 1 FROM wallets WHERE account_id = $1 AND tenant_id = $2 FOR UPDATE")
            .bind(&self.account_id)
            .bind(tenant)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx
    }

    /// (available_credits, reserved_credits) straight from the database.
    pub async fn balances(&self, tenant: &str) -> (i64, i64) {
        balances(&self.pool, &self.account_id, tenant).await
    }

    /// (available, reserved, expiring) straight from the database.
    pub async fn wallet_state(&self, tenant: &str) -> (i64, i64, i64) {
        sqlx::query_as(
            "SELECT available_credits, reserved_credits, expiring_credits FROM wallets \
             WHERE account_id = $1 AND tenant_id = $2",
        )
        .bind(&self.account_id)
        .bind(tenant)
        .fetch_one(&self.pool)
        .await
        .expect("wallet row missing")
    }

    /// A bucket's `remaining` credits.
    pub async fn bucket_remaining(&self, bucket_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT remaining FROM credit_buckets WHERE id = $1")
            .bind(bucket_id)
            .fetch_one(&self.pool)
            .await
            .expect("bucket row missing")
    }

    /// Every credit granted into a bucket is accounted for:
    /// granted = remaining + held by pending reservations + consumed
    /// (COMMIT/OVERAGE_DEDUCT) + expired (CREDIT_EXPIRE). The wallet's
    /// `expiring_credits` and `next_expiry_at` match its buckets.
    pub async fn assert_buckets_reconcile(&self, tenant: &str) {
        let rows: Vec<(Uuid, i64, i64, i64, i64, i64)> = sqlx::query_as(
            "WITH consumed AS ( \
               SELECT (x->>'bucket_id')::uuid AS id, SUM((x->>'amount')::bigint)::bigint AS amount \
               FROM ledger_entries l, jsonb_array_elements(l.metadata->'buckets') x \
               WHERE l.account_id = $1 AND l.tenant_id = $2 \
                 AND l.entry_type IN ('COMMIT', 'OVERAGE_DEDUCT') GROUP BY 1), \
             expired AS ( \
               SELECT bucket_id AS id, SUM(-delta_available)::bigint AS amount FROM ledger_entries \
               WHERE account_id = $1 AND tenant_id = $2 AND entry_type = 'CREDIT_EXPIRE' GROUP BY 1), \
             held AS ( \
               SELECT (x->>'bucket_id')::uuid AS id, SUM((x->>'amount')::bigint)::bigint AS amount \
               FROM reservations r, jsonb_array_elements(r.bucket_holds) x \
               WHERE r.account_id = $1 AND r.tenant_id = $2 AND r.status = 'PENDING' GROUP BY 1) \
             SELECT b.id, b.granted, b.remaining, COALESCE(h.amount, 0), COALESCE(c.amount, 0), \
                    COALESCE(e.amount, 0) \
             FROM credit_buckets b \
             LEFT JOIN held h ON h.id = b.id LEFT JOIN consumed c ON c.id = b.id \
             LEFT JOIN expired e ON e.id = b.id \
             WHERE b.account_id = $1 AND b.tenant_id = $2",
        )
        .bind(&self.account_id)
        .bind(tenant)
        .fetch_all(&self.pool)
        .await
        .unwrap();
        for (id, granted, remaining, held, consumed, expired) in &rows {
            assert_eq!(
                *granted,
                remaining + held + consumed + expired,
                "bucket {id}: granted {granted} != remaining {remaining} + held {held} + consumed {consumed} + expired {expired}"
            );
        }
        let (expiring, next_expiry, sum, min_expiry): (i64, Option<DateTime<Utc>>, i64, Option<DateTime<Utc>>) =
            sqlx::query_as(
                "SELECT w.expiring_credits, w.next_expiry_at, \
                        COALESCE((SELECT SUM(remaining) FROM credit_buckets b \
                          WHERE b.account_id = w.account_id AND b.tenant_id = w.tenant_id), 0)::bigint, \
                        (SELECT MIN(expires_at) FROM credit_buckets b \
                          WHERE b.account_id = w.account_id AND b.tenant_id = w.tenant_id AND remaining > 0) \
                 FROM wallets w WHERE w.account_id = $1 AND w.tenant_id = $2",
            )
            .bind(&self.account_id)
            .bind(tenant)
            .fetch_one(&self.pool)
            .await
            .unwrap();
        assert_eq!(expiring, sum, "expiring_credits must equal the buckets' remaining credits");
        assert_eq!(next_expiry, min_expiry, "next_expiry_at must be the earliest live bucket's expiry");
    }

    /// (status, committed_amount) of a reservation.
    pub async fn reservation(&self, id: Uuid) -> (String, Option<i64>) {
        sqlx::query_as("SELECT status, committed_amount FROM reservations WHERE id = $1")
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .expect("reservation row missing")
    }

    pub async fn count_reservations(&self, tenant: &str) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM reservations WHERE account_id = $1 AND tenant_id = $2")
            .bind(&self.account_id)
            .bind(tenant)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    pub async fn count_ledger(&self, tenant: &str, entry_type: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM ledger_entries WHERE account_id = $1 AND tenant_id = $2 AND entry_type = $3",
        )
        .bind(&self.account_id)
        .bind(tenant)
        .bind(entry_type)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    /// (entry_type, delta_available, delta_reserved) for a reservation, oldest first.
    pub async fn ledger_for(&self, reservation_id: Uuid) -> Vec<(String, i64, i64)> {
        sqlx::query_as(
            "SELECT entry_type, delta_available, delta_reserved FROM ledger_entries \
             WHERE reservation_id = $1 ORDER BY created_at, id",
        )
        .bind(reservation_id)
        .fetch_all(&self.pool)
        .await
        .unwrap()
    }

    /// The wallet must always equal the sum of its ledger deltas.
    pub async fn assert_ledger_reconciles(&self, tenant: &str) {
        let (sum_available, sum_reserved): (i64, i64) = sqlx::query_as(
            "SELECT COALESCE(SUM(delta_available), 0)::bigint, COALESCE(SUM(delta_reserved), 0)::bigint \
             FROM ledger_entries WHERE account_id = $1 AND tenant_id = $2",
        )
        .bind(&self.account_id)
        .bind(tenant)
        .fetch_one(&self.pool)
        .await
        .unwrap();
        assert_eq!(
            self.balances(tenant).await,
            (sum_available, sum_reserved),
            "wallet does not reconcile with its ledger"
        );
    }
}

pub async fn balances(pool: &PgPool, account_id: &str, tenant: &str) -> (i64, i64) {
    sqlx::query_as(
        "SELECT available_credits, reserved_credits FROM wallets WHERE account_id = $1 AND tenant_id = $2",
    )
    .bind(account_id)
    .bind(tenant)
    .fetch_one(pool)
    .await
    .expect("wallet row missing")
}

pub fn reservation_id(res: &TestResponse) -> Uuid {
    res.body["reservation_id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no reservation_id in {}", res.body))
}
