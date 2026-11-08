use std::{env, net::SocketAddr, time::Duration};

use crate::{auth, reaper};

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub bind_addr: SocketAddr,
    pub max_connections: u32,
    pub reaper_interval: Duration,
    pub auth_cache_ttl: Duration,
    pub usage_flush_interval: Duration,
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
            Err(_) => 20,
        };
        Ok(Self {
            database_url,
            bind_addr,
            max_connections,
            reaper_interval: millis("REAPER_INTERVAL_MS", reaper::DEFAULT_INTERVAL)?,
            auth_cache_ttl: millis("AUTH_CACHE_TTL_MS", auth::DEFAULT_AUTH_CACHE_TTL)?,
            usage_flush_interval: millis("USAGE_FLUSH_INTERVAL_MS", auth::DEFAULT_USAGE_FLUSH_INTERVAL)?,
        })
    }
}
