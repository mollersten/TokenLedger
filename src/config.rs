use std::{env, net::SocketAddr, time::Duration};

use crate::reaper;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub bind_addr: SocketAddr,
    pub max_connections: u32,
    pub reaper_interval: Duration,
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
        let reaper_interval = match env::var("REAPER_INTERVAL_MS") {
            Ok(v) => Duration::from_millis(
                v.parse().map_err(|e| format!("invalid REAPER_INTERVAL_MS: {e}"))?,
            ),
            Err(_) => reaper::DEFAULT_INTERVAL,
        };
        Ok(Self { database_url, bind_addr, max_connections, reaper_interval })
    }
}
