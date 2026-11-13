//! Background reaper:
//! - returns credits held by reservations whose client never called
//!   `/commit` or `/release` (e.g. it crashed);
//! - expires credit buckets that are past their `expires_at`.
//!
//! The reservation pass runs one transaction:
//!   1. `SELECT ... WHERE status = 'PENDING' AND expires_at < NOW()
//!       LIMIT 100 FOR UPDATE SKIP LOCKED` — several reaper instances can run
//!      side by side and never pick the same rows.
//!   2. For each tenant, lock its wallet with `FOR UPDATE SKIP LOCKED`. If a
//!      request handler holds it, skip that tenant this pass instead of
//!      waiting: handlers lock wallet -> reservation, so waiting here while
//!      holding reservation locks could deadlock. Handlers that reach an
//!      overdue reservation expire it themselves, so nothing is lost.
//!   3. Move `reserved_amount` back to `available_credits` (and to the
//!      buckets that funded it), mark the reservation EXPIRED and append an
//!      EXPIRE ledger entry.
//!
//! The bucket pass then finds wallets with due buckets, locks each with
//! `SKIP LOCKED` (no other locks are held, so it never waits), writes a
//! CREDIT_EXPIRE entry per due bucket that still had credits and marks every
//! due bucket settled. A busy wallet is left for the next pass; the request
//! holding it expires due buckets itself.

use std::{collections::HashMap, time::Duration};

use sqlx::PgPool;
use tokio::{task::JoinHandle, time::MissedTickBehavior};

use crate::{
    error::ApiError,
    ledger::{self, Op},
    models::{RESERVATION_COLUMNS, Reservation},
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
    /// Wallets found with buckets past their expiry.
    pub bucket_wallets: usize,
    /// Buckets expired.
    pub buckets_expired: usize,
    /// Wallets with due buckets left for a later pass because they were busy.
    pub bucket_wallets_skipped: usize,
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
                        if stats.selected > 0 || stats.bucket_wallets > 0 {
                            tracing::info!(
                                expired = stats.expired,
                                skipped = stats.skipped,
                                buckets_expired = stats.buckets_expired,
                                bucket_wallets_skipped = stats.bucket_wallets_skipped,
                                "reaper pass"
                            );
                        }
                        let full = BATCH_SIZE as usize;
                        if stats.expired < full && stats.buckets_expired < full {
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

/// Run a single reaper pass: reservations first, then credit buckets.
pub async fn run_once(pool: &PgPool, batch_size: i64) -> Result<ReapStats, ApiError> {
    let mut stats = expire_reservations(pool, batch_size).await?;
    let (wallets, expired, skipped) = expire_buckets(pool, batch_size).await?;
    stats.bucket_wallets = wallets;
    stats.buckets_expired = expired;
    stats.bucket_wallets_skipped = skipped;
    Ok(stats)
}

async fn expire_reservations(pool: &PgPool, batch_size: i64) -> Result<ReapStats, ApiError> {
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

    let mut wallets: HashMap<(String, String), Option<Op>> = HashMap::new();
    for reservation in &due {
        let wallet_key = (reservation.account_id.clone(), reservation.tenant_id.clone());
        if !wallets.contains_key(&wallet_key) {
            let op = match ledger::try_lock_wallet(&mut tx, &reservation.account_id, &reservation.tenant_id).await? {
                Some(locked) => Some(Op::begin(&mut tx, locked, "reaper").await?),
                None => None,
            };
            wallets.insert(wallet_key.clone(), op);
        }
        match wallets.get_mut(&wallet_key).and_then(Option::as_mut) {
            Some(op) => {
                ledger::expire_reservation(op, reservation, "reaper")?;
                op.flush(&mut tx).await?;
                stats.expired += 1;
            }
            None => stats.skipped += 1,
        }
    }

    tx.commit().await?;
    Ok(stats)
}

/// Returns (wallets with due buckets, buckets expired, wallets skipped).
async fn expire_buckets(pool: &PgPool, batch_size: i64) -> Result<(usize, usize, usize), ApiError> {
    let mut tx = pool.begin().await?;
    let due: Vec<(String, String)> = sqlx::query_as(
        "SELECT DISTINCT account_id, tenant_id FROM ( \
           SELECT account_id, tenant_id FROM credit_buckets \
           WHERE settled_at IS NULL AND expires_at <= NOW() \
           ORDER BY expires_at LIMIT $1) due",
    )
    .bind(batch_size)
    .fetch_all(&mut *tx)
    .await?;

    let (mut expired, mut skipped) = (0, 0);
    for (account_id, tenant_id) in &due {
        match ledger::try_lock_wallet(&mut tx, account_id, tenant_id).await? {
            Some(locked) => {
                let mut op = Op::new(locked);
                expired += op.expire_due_buckets(&mut tx, "reaper").await?;
                op.flush(&mut tx).await?;
            }
            None => skipped += 1,
        }
    }
    tx.commit().await?;
    Ok((due.len(), expired, skipped))
}
