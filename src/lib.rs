//! TokenLedger: atomic usage metering, credit ledger & entitlements API.

pub mod config;
pub mod error;
pub mod idempotency;
pub mod ledger;
pub mod models;
pub mod reaper;
pub mod routes;

use std::time::Duration;

use sqlx::{PgPool, migrate::Migrator, postgres::PgPoolOptions};

pub use routes::router;

pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

impl AppState {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

pub async fn connect(database_url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(10))
        .connect(database_url)
        .await
}
