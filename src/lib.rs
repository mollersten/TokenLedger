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

use sqlx::{
    PgPool,
    migrate::Migrator,
    postgres::{PgConnectOptions, PgPoolOptions},
};

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

/// Where a connection string points, without credentials:
/// `host:port/database`. Safe to log.
pub fn database_label(database_url: &str) -> String {
    match database_url.parse::<PgConnectOptions>() {
        Ok(o) => format!("{}:{}/{}", o.get_host(), o.get_port(), o.get_database().unwrap_or("-")),
        Err(_) => "an invalid connection string".to_owned(),
    }
}

/// True when the connection string points at this machine (localhost, a
/// loopback address or a Unix socket). Tests and the load test create and
/// change data, so they refuse anything else.
pub fn is_local_database(database_url: &str) -> bool {
    database_url.parse::<PgConnectOptions>().is_ok_and(|o| {
        let host = o.get_host();
        matches!(host, "localhost" | "127.0.0.1" | "::1" | "[::1]") || host.starts_with('/')
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_label_hides_credentials() {
        let url = "postgresql://postgres.abc:s3cret@aws-0-eu-west-1.pooler.supabase.com:5432/postgres";
        assert_eq!(database_label(url), "aws-0-eu-west-1.pooler.supabase.com:5432/postgres");
        assert_eq!(database_label("not a url"), "an invalid connection string");
    }

    #[test]
    fn only_local_databases_count_as_local() {
        assert!(is_local_database("postgres://postgres:postgres@localhost:5432/tokenledger"));
        assert!(is_local_database("postgres://u:p@127.0.0.1/db"));
        assert!(is_local_database("postgres://u:p@[::1]:5432/db"));
        assert!(!is_local_database("postgresql://postgres.abc:pw@aws-0-eu-west-1.pooler.supabase.com:5432/postgres"));
        assert!(!is_local_database("postgres://u:p@localhost.evil.example/db"));
        assert!(!is_local_database("garbage"));
    }
}
