use std::{env, net::SocketAddr, time::Duration};

use crate::{auth, idempotency, import, reaper};

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub bind_addr: SocketAddr,
    pub max_connections: u32,
    pub reaper_interval: Duration,
    pub auth_cache_ttl: Duration,
    pub usage_flush_interval: Duration,
    pub import_caps: import::ImportCaps,
    /// `None` keeps idempotency records forever.
    pub idempotency_retention: Option<Duration>,
}

/// Supabase's session pooler allows 15 sessions on the smallest compute; 10
/// leaves room for the admin CLI and a second instance during a deploy (set
/// `DATABASE_MAX_CONNECTIONS` lower when running several instances).
pub const DEFAULT_MAX_CONNECTIONS: u32 = 10;

fn positive(name: &str, default: i64) -> Result<i64, String> {
    match env::var(name) {
        Ok(v) => match v.parse::<i64>() {
            Ok(n) if n > 0 => Ok(n),
            _ => Err(format!("{name} must be a whole number greater than 0")),
        },
        Err(_) => Ok(default),
    }
}

fn millis(name: &str, default: Duration) -> Result<Duration, String> {
    match env::var(name) {
        Ok(v) => Ok(Duration::from_millis(v.parse().map_err(|e| format!("invalid {name}: {e}"))?)),
        Err(_) => Ok(default),
    }
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let database_url = env::var("DATABASE_URL")
            .map_err(|_| "DATABASE_URL must be set (see .env.example)".to_owned())?;
        let bind_addr = env::var("BIND_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:8080".to_owned())
            .parse()
            .map_err(|e| format!("invalid BIND_ADDR: {e}"))?;
        let max_connections = match env::var("DATABASE_MAX_CONNECTIONS") {
            Ok(v) => v.parse().map_err(|e| format!("invalid DATABASE_MAX_CONNECTIONS: {e}"))?,
            Err(_) => DEFAULT_MAX_CONNECTIONS,
        };
        Ok(Self {
            database_url,
            bind_addr,
            max_connections,
            reaper_interval: millis("REAPER_INTERVAL_MS", reaper::DEFAULT_INTERVAL)?,
            auth_cache_ttl: millis("AUTH_CACHE_TTL_MS", auth::DEFAULT_AUTH_CACHE_TTL)?,
            usage_flush_interval: millis("USAGE_FLUSH_INTERVAL_MS", auth::DEFAULT_USAGE_FLUSH_INTERVAL)?,
            idempotency_retention: match env::var("IDEMPOTENCY_RETENTION_HOURS") {
                Err(_) => Some(idempotency::DEFAULT_RETENTION),
                Ok(v) => match v.parse::<u32>() {
                    Ok(0) => None,
                    Ok(hours) => Some(Duration::from_secs(u64::from(hours) * 3600)),
                    Err(_) => return Err("IDEMPOTENCY_RETENTION_HOURS must be 0 (keep forever) or a number of hours".to_owned()),
                },
            },
            import_caps: import::ImportCaps {
                customers_per_day: positive("IMPORT_CUSTOMERS_PER_DAY", import::DEFAULT_CUSTOMERS_PER_DAY)?,
                purchases_per_day: positive("IMPORT_PURCHASES_PER_DAY", import::DEFAULT_PURCHASES_PER_DAY)?,
            },
        })
    }
}
