//! Shared integration-test harness: builds the real router on a real
//! PostgreSQL database and drives it in-process with `tower::ServiceExt`.
//!
//! Uses `TEST_DATABASE_URL` (never `DATABASE_URL`, so tests can't hit a
//! remote database by accident), defaulting to the local dev database from
//! `scripts/local-pg.sh` / `docker-compose.yml`. Each test uses unique tenant
//! ids, so runs are repeatable against a persistent database.
#![allow(dead_code)]

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use backend::{AppState, router};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

pub const DEFAULT_TEST_DATABASE_URL: &str = "postgres://postgres:postgres@localhost:5432/tokenledger";

pub struct TestApp {
    pub router: Router,
    pub pool: PgPool,
}

#[derive(Debug, Clone)]
pub struct TestResponse {
    pub status: StatusCode,
    pub body: Value,
    pub replayed: bool,
}

pub fn unique_tenant(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

pub fn new_key() -> String {
    Uuid::new_v4().to_string()
}

pub async fn send(
    router: Router,
    method: Method,
    path: &str,
    idempotency_key: Option<&str>,
    body: Option<Value>,
) -> TestResponse {
    let mut builder = Request::builder().method(method).uri(path);
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
    let response = router.oneshot(request).await.expect("router call failed");
    let status = response.status();
    let replayed = response.headers().contains_key("idempotent-replayed");
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
    };
    TestResponse { status, body, replayed }
}

pub async fn post(router: Router, path: &str, key: &str, body: Value) -> TestResponse {
    send(router, Method::POST, path, Some(key), Some(body)).await
}

impl TestApp {
    pub async fn spawn() -> Self {
        let _ = dotenvy::dotenv();
        let url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| DEFAULT_TEST_DATABASE_URL.to_owned());
        let pool = backend::connect(&url, 30)
            .await
            .expect("cannot reach the test database; start it with `scripts/local-pg.sh start`");
        backend::MIGRATOR.run(&pool).await.expect("migrations failed");
        let router = router(AppState::new(pool.clone()));
        Self { router, pool }
    }

    pub async fn post(&self, path: &str, key: &str, body: Value) -> TestResponse {
        post(self.router.clone(), path, key, body).await
    }

    pub async fn get(&self, path: &str) -> TestResponse {
        send(self.router.clone(), Method::GET, path, None, None).await
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

    /// (available_credits, reserved_credits) straight from the database.
    pub async fn balances(&self, tenant: &str) -> (i64, i64) {
        sqlx::query_as("SELECT available_credits, reserved_credits FROM wallets WHERE tenant_id = $1")
            .bind(tenant)
            .fetch_one(&self.pool)
            .await
            .expect("wallet row missing")
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
        sqlx::query_scalar("SELECT COUNT(*) FROM reservations WHERE tenant_id = $1")
            .bind(tenant)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    pub async fn count_ledger(&self, tenant: &str, entry_type: &str) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM ledger_entries WHERE tenant_id = $1 AND entry_type = $2")
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
             FROM ledger_entries WHERE tenant_id = $1",
        )
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

pub fn reservation_id(res: &TestResponse) -> Uuid {
    res.body["reservation_id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no reservation_id in {}", res.body))
}
