//! Background TTL reaper: returns credits held by reservations whose client
//! never called `/commit` or `/release` (e.g. it crashed).
//!
//! Each pass runs one transaction:
//!   1. `SELECT ... WHERE status = 'PENDING' AND expires_at < NOW()
//!       LIMIT 100 FOR UPDATE SKIP LOCKED` — several reaper instances can run
//!      side by side and never pick the same rows.
//!   2. For each tenant, lock its wallet with `FOR UPDATE SKIP LOCKED`. If a
//!      request handler holds it, skip that tenant this pass instead of
//!      waiting: handlers lock wallet -> reservation, so waiting here while
//!      holding reservation locks could deadlock. Handlers that reach an
//!      overdue reservation expire it themselves, so nothing is lost.
//!   3. Move `reserved_amount` back to `available_credits`, mark the
//!      reservation EXPIRED and append an EXPIRE ledger entry.

use std::{collections::HashMap, time::Duration};

use sqlx::PgPool;
use tokio::{task::JoinHandle, time::MissedTickBehavior};

use crate::{
    error::ApiError,
    ledger,
    models::{RESERVATION_COLUMNS, Reservation, Wallet},
};

pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(2);
pub const BATCH_SIZE: i64 = 100;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReapStats {
    /// Expired reservations locked by this pass.
    pub selected: usize,
    /// Reservations transitioned to EXPIRED.
    pub expired: usize,
    /// Reservations left for a later pass because their wallet was busy.
    pub skipped: usize,
}

/// Spawn the reaper loop. Abort the handle to stop it; an in-flight pass is
/// rolled back when its transaction is dropped.
pub fn spawn(pool: PgPool, interval: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            // Keep draining while passes come back full.
            loop {
                match run_once(&pool, BATCH_SIZE).await {
                    Ok(stats) => {
                        if stats.selected > 0 {
                            tracing::info!(
                                expired = stats.expired,
                                skipped = stats.skipped,
                                "reaper pass"
                            );
                        }
                        if stats.expired < BATCH_SIZE as usize {
                            break;
                        }
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "reaper pass failed; retrying next tick");
                        break;
                    }
                }
            }
        }
    })
}

/// Run a single reaper pass.
pub async fn run_once(pool: &PgPool, batch_size: i64) -> Result<ReapStats, ApiError> {
    let mut tx = pool.begin().await?;

    let due = sqlx::query_as::<_, Reservation>(&format!(
        "SELECT {RESERVATION_COLUMNS} FROM reservations \
         WHERE status = 'PENDING' AND expires_at < NOW() \
         ORDER BY expires_at \
         LIMIT $1 \
         FOR UPDATE SKIP LOCKED"
    ))
    .bind(batch_size)
    .fetch_all(&mut *tx)
    .await?;

    let mut stats = ReapStats { selected: due.len(), ..ReapStats::default() };
    if due.is_empty() {
        tx.commit().await?;
        return Ok(stats);
    }

    let mut wallets: HashMap<String, Option<Wallet>> = HashMap::new();
    for reservation in &due {
        if !wallets.contains_key(&reservation.tenant_id) {
            let wallet = ledger::try_lock_wallet(&mut tx, &reservation.tenant_id).await?;
            wallets.insert(reservation.tenant_id.clone(), wallet);
        }
        match wallets.get_mut(&reservation.tenant_id).and_then(Option::as_mut) {
            Some(wallet) => {
                ledger::expire_reservation(&mut tx, wallet, reservation, "reaper").await?;
                stats.expired += 1;
            }
            None => stats.skipped += 1,
        }
    }

    tx.commit().await?;
    Ok(stats)
}
