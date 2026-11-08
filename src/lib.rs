//! TokenLedger: atomic usage metering, credit ledger & entitlements API.

pub mod accounts;
pub mod auth;
pub mod config;
pub mod error;
pub mod idempotency;
pub mod ledger;
pub mod models;
pub mod reaper;
pub mod routes;

use std::{sync::Arc, time::Duration};

use sqlx::{PgPool, migrate::Migrator, postgres::PgPoolOptions};

pub use routes::router;

pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub auth: Arc<auth::AuthCache>,
    pub limiter: Arc<auth::RateLimiter>,
    pub usage: Arc<auth::UsageMeter>,
}

impl AppState {
    pub fn new(pool: PgPool) -> Self {
        Self::with_auth_cache_ttl(pool, auth::DEFAULT_AUTH_CACHE_TTL)
    }

    /// A zero TTL disables the key cache (every request checks the database).
    pub fn with_auth_cache_ttl(pool: PgPool, ttl: Duration) -> Self {
        Self {
            pool,
            auth: Arc::new(auth::AuthCache::new(ttl)),
            limiter: Arc::new(auth::RateLimiter::default()),
            usage: Arc::new(auth::UsageMeter::default()),
        }
    }
}

pub async fn connect(database_url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(10))
        .connect(database_url)
        .await
}
