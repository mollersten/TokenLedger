//! TokenLedger: atomic usage metering, credit ledger & entitlements API.

pub mod accounts;
pub mod auth;
pub mod config;
pub mod error;
pub mod gate;
pub mod idempotency;
pub mod import;
pub mod ledger;
pub mod models;
pub mod reaper;
pub mod routes;

use std::{sync::Arc, time::Duration};

use sqlx::{
    Executor, PgPool,
    migrate::Migrator,
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
};

pub use routes::router;

pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub auth: Arc<auth::AuthCache>,
    pub limiter: Arc<auth::RateLimiter>,
    pub usage: Arc<auth::UsageMeter>,
    pub gates: Arc<gate::Gates>,
    pub import_caps: import::ImportCaps,
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
            gates: Arc::new(gate::Gates::default()),
            import_caps: import::ImportCaps::default(),
        }
    }

    pub fn with_import_caps(mut self, caps: import::ImportCaps) -> Self {
        self.import_caps = caps;
        self
    }
}

/// How long a statement may wait for a row lock before it fails (503 to the
/// client). Bounds how long a request can sit on a pooled connection.
pub const LOCK_TIMEOUT: &str = "5s";
/// The longest any single statement may run.
pub const STATEMENT_TIMEOUT: &str = "60s";
/// A transaction left open this long without activity is closed, releasing
/// its locks.
pub const IDLE_IN_TRANSACTION_TIMEOUT: &str = "60s";

/// Open a pool. Every connection gets the timeouts above, and a database
/// that is not on this machine is only reached over TLS unless the
/// connection string says otherwise with `sslmode=`.
pub async fn connect(database_url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    let mut options: PgConnectOptions = database_url.parse()?;
    if !is_local_database(database_url) && !database_url.contains("sslmode=") {
        options = options.ssl_mode(PgSslMode::Require);
    }
    PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(10))
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                conn.execute(
                    format!(
                        "SET lock_timeout = '{LOCK_TIMEOUT}'; SET statement_timeout = '{STATEMENT_TIMEOUT}'; \
                         SET idle_in_transaction_session_timeout = '{IDLE_IN_TRANSACTION_TIMEOUT}'"
                    )
                    .as_str(),
                )
                .await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await
}

/// Apply pending migrations. They run on one connection without the
/// statement timeout (a migration that rewrites a large table can take
/// longer than any request should), but keep the lock timeout: a migration
/// stuck behind live traffic fails fast instead of stalling every request.
///
/// A lock wait that times out is retried, for up to a minute: that is
/// another instance migrating at the same moment (sqlx serialises migrations
/// with an advisory lock, which the lock timeout applies to as well), or a
/// long transaction in the way of a schema change.
pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    const ATTEMPTS: u32 = 20;
    let mut conn = pool.acquire().await?;
    conn.execute("SET statement_timeout = 0").await?;
    let mut attempt = 1;
    let result = loop {
        let result = MIGRATOR.run(&mut *conn).await;
        // The advisory lock fails as `Execute`; a migration's own statement as
        // `ExecuteMigration`.
        let lock_timed_out = match &result {
            Err(
                sqlx::migrate::MigrateError::Execute(sqlx::Error::Database(db))
                | sqlx::migrate::MigrateError::ExecuteMigration(sqlx::Error::Database(db), _),
            ) => db.code().as_deref() == Some("55P03"),
            _ => false,
        };
        if !lock_timed_out || attempt == ATTEMPTS {
            break result;
        }
        tracing::warn!(attempt, "migration waited too long for a lock (another instance migrating?); retrying");
        attempt += 1;
        tokio::time::sleep(Duration::from_secs(3)).await;
    };
    // Hand the connection back to the pool with its usual timeout.
    conn.execute(format!("SET statement_timeout = '{STATEMENT_TIMEOUT}'").as_str()).await?;
    result
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
