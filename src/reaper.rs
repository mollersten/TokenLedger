//! Background reaper:
//! - returns credits held by reservations whose client never called
//!   `/commit` or `/release` (e.g. it crashed);
//! - expires credit buckets that are past their `expires_at`.
//!
//! A pass first selects due work (`ORDER BY expires_at LIMIT batch_size`)
//! without locking anything, then handles it one wallet at a time, each in a
//! transaction of its own, the same way a request does:
//!   1. Lock the wallet with `FOR UPDATE SKIP LOCKED`. If a request (or
//!      another reaper) holds it, skip the wallet this pass instead of
//!      waiting. Requests that reach an overdue reservation or a due bucket
//!      expire it themselves, so nothing is lost.
//!   2. Re-read the selected reservations under that lock, keeping those
//!      still PENDING (a request may have settled some in the meantime).
//!   3. Reservations: move `reserved_amount` back to `available_credits` (and
//!      to the buckets that funded it), mark the reservation EXPIRED and
//!      append an EXPIRE ledger entry. Buckets: write a CREDIT_EXPIRE entry
//!      per due bucket that still had credits and mark every due bucket
//!      settled.
//!   4. Commit.
//!
//! The reaper takes the wallet lock first and only then touches the
//! wallet's reservations and buckets, the same lock order as handlers.
//!
//! # Failures
//!
//! One wallet's failure stays with that wallet. If its work fails (say its
//! buckets no longer add up to its `expiring_credits`), its transaction is
//! rolled back, the error is logged with its `account_id` and `tenant_id`,
//! it is counted in [`ReapStats`], and the pass moves on to the next wallet.
//! A pass returns `Err` only when the database itself fails: no connection
//! from the pool, a failed selection, or a rollback that fails.
//!
//! Each wallet gets a transaction rather than a savepoint inside one
//! transaction per pass. That way the reaper holds a wallet lock only while
//! it works on that wallet, and no pass builds up a subtransaction per
//! wallet (more than 64 in one transaction slows snapshots for every
//! backend).
//!
//! Wallets that are busy or failing do not use up the batch: their rows are
//! left out and the pass selects again, until it has handled `batch_size`
//! rows or nothing else is due. So the earliest-due rows, which are selected
//! first every time, cannot keep the rest waiting even when they fill a
//! whole batch. [`spawn`] also leaves failing wallets out of the passes that
//! drain a backlog within one tick, so each is retried (and logged) once per
//! tick.

use std::{collections::HashMap, time::Duration};

use sqlx::{PgConnection, PgPool};
use tokio::{task::JoinHandle, time::MissedTickBehavior};
use uuid::Uuid;

use crate::{
    error::ApiError,
    ledger::{self, Op},
    models::{RESERVATION_COLUMNS, Reservation},
};

pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(2);
pub const BATCH_SIZE: i64 = 100;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReapStats {
    /// Due reservations selected by this pass.
    pub selected: usize,
    /// Reservations transitioned to EXPIRED.
    pub expired: usize,
    /// Reservations left for a later pass because their wallet was busy.
    pub skipped: usize,
    /// Wallets whose due reservations could not be expired: the wallet's
    /// work was rolled back and the error logged, and its reservations are
    /// left for a later pass.
    pub wallets_failed: usize,
    /// Wallets found with buckets past their expiry.
    pub bucket_wallets: usize,
    /// Buckets expired.
    pub buckets_expired: usize,
    /// Wallets with due buckets left for a later pass because they were busy.
    pub bucket_wallets_skipped: usize,
    /// Wallets with due buckets left for a later pass because they failed
    /// (rolled back and logged).
    pub bucket_wallets_failed: usize,
    /// A pass stopped because it had handled a whole batch, not because
    /// nothing else was due, so more may be due right away.
    pub batch_full: bool,
}

/// Spawn the reaper loop. Abort the handle to stop it; an in-flight wallet
/// transaction is rolled back when it is dropped.
pub fn spawn(pool: PgPool, interval: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            // Keep draining while passes fill their batch. Wallets that fail
            // are left out until the next tick.
            let mut failing = Failing::default();
            loop {
                match run_pass(&pool, BATCH_SIZE, &mut failing).await {
                    Ok(stats) => {
                        if stats.selected > 0 || stats.bucket_wallets > 0 {
                            tracing::info!(
                                expired = stats.expired,
                                skipped = stats.skipped,
                                wallets_failed = stats.wallets_failed,
                                buckets_expired = stats.buckets_expired,
                                bucket_wallets_skipped = stats.bucket_wallets_skipped,
                                bucket_wallets_failed = stats.bucket_wallets_failed,
                                "reaper pass"
                            );
                        }
                        if !stats.batch_full {
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
    run_pass(pool, batch_size, &mut Failing::default()).await
}

/// A pass that leaves out the wallets in `failing` and adds the wallets that
/// fail in it.
async fn run_pass(pool: &PgPool, batch_size: i64, failing: &mut Failing) -> Result<ReapStats, ApiError> {
    let mut stats = ReapStats::default();
    expire_reservations(pool, batch_size, &mut failing.reservations, &mut stats).await?;
    expire_buckets(pool, batch_size, &mut failing.buckets, &mut stats).await?;
    Ok(stats)
}

/// `(account_id, tenant_id)`
type WalletKey = (String, String);

/// Wallets to leave out of a selection, as parallel arrays for `UNNEST`.
#[derive(Debug, Default, Clone)]
struct Wallets {
    account_ids: Vec<String>,
    tenant_ids: Vec<String>,
}

impl Wallets {
    fn push(&mut self, (account_id, tenant_id): &WalletKey) {
        self.account_ids.push(account_id.clone());
        self.tenant_ids.push(tenant_id.clone());
    }
}

/// Wallets that failed earlier in this tick, per kind of pass.
#[derive(Debug, Default)]
struct Failing {
    reservations: Wallets,
    buckets: Wallets,
}

async fn expire_reservations(
    pool: &PgPool,
    batch_size: i64,
    failing: &mut Wallets,
    stats: &mut ReapStats,
) -> Result<(), ApiError> {
    // Failing wallets, plus wallets found busy in this pass.
    let mut left_out = failing.clone();
    let mut budget = batch_size;
    loop {
        let due: Vec<(Uuid, String, String)> = sqlx::query_as(
            "SELECT id, account_id, tenant_id FROM reservations \
             WHERE status = 'PENDING' AND expires_at < NOW() \
               AND (account_id, tenant_id) NOT IN (SELECT * FROM UNNEST($2::text[], $3::text[])) \
             ORDER BY expires_at \
             LIMIT $1",
        )
        .bind(budget)
        .bind(&left_out.account_ids)
        .bind(&left_out.tenant_ids)
        .fetch_all(pool)
        .await?;
        stats.selected += due.len();
        let exhausted = (due.len() as i64) < budget;

        for (wallet, ids) in group_by_wallet(due) {
            match reap_wallet(pool, &wallet, WalletJob::Reservations(&ids)).await? {
                WalletOutcome::Done(expired) => {
                    stats.expired += expired;
                    budget -= ids.len() as i64;
                }
                WalletOutcome::Busy => {
                    stats.skipped += ids.len();
                    left_out.push(&wallet);
                }
                WalletOutcome::Failed => {
                    stats.wallets_failed += 1;
                    left_out.push(&wallet);
                    failing.push(&wallet);
                }
            }
        }

        if exhausted {
            return Ok(());
        }
        if budget == 0 {
            stats.batch_full = true;
            return Ok(());
        }
        // The batch was full but some of it went to busy or failing wallets:
        // select again without them.
    }
}

async fn expire_buckets(
    pool: &PgPool,
    batch_size: i64,
    failing: &mut Wallets,
    stats: &mut ReapStats,
) -> Result<(), ApiError> {
    let mut left_out = failing.clone();
    let mut budget = batch_size;
    loop {
        // Wallets of the next `budget` due buckets, with how many each has.
        let due: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT account_id, tenant_id, COUNT(*) FROM ( \
               SELECT account_id, tenant_id, expires_at FROM credit_buckets \
               WHERE settled_at IS NULL AND expires_at <= NOW() \
                 AND (account_id, tenant_id) NOT IN (SELECT * FROM UNNEST($2::text[], $3::text[])) \
               ORDER BY expires_at LIMIT $1) due \
             GROUP BY account_id, tenant_id \
             ORDER BY MIN(expires_at)",
        )
        .bind(budget)
        .bind(&left_out.account_ids)
        .bind(&left_out.tenant_ids)
        .fetch_all(pool)
        .await?;
        stats.bucket_wallets += due.len();
        let exhausted = due.iter().map(|(_, _, buckets)| buckets).sum::<i64>() < budget;

        for (account_id, tenant_id, buckets) in due {
            let wallet = (account_id, tenant_id);
            match reap_wallet(pool, &wallet, WalletJob::Buckets).await? {
                WalletOutcome::Done(expired) => {
                    stats.buckets_expired += expired;
                    budget -= buckets;
                }
                WalletOutcome::Busy => {
                    stats.bucket_wallets_skipped += 1;
                    left_out.push(&wallet);
                }
                WalletOutcome::Failed => {
                    stats.bucket_wallets_failed += 1;
                    left_out.push(&wallet);
                    failing.push(&wallet);
                }
            }
        }

        if exhausted {
            return Ok(());
        }
        if budget == 0 {
            stats.batch_full = true;
            return Ok(());
        }
    }
}

/// Group selected rows by wallet, wallets in the order they first appear.
fn group_by_wallet(rows: Vec<(Uuid, String, String)>) -> Vec<(WalletKey, Vec<Uuid>)> {
    let mut index: HashMap<WalletKey, usize> = HashMap::new();
    let mut groups: Vec<(WalletKey, Vec<Uuid>)> = Vec::new();
    for (id, account_id, tenant_id) in rows {
        let wallet = (account_id, tenant_id);
        match index.get(&wallet) {
            Some(&i) => groups[i].1.push(id),
            None => {
                index.insert(wallet.clone(), groups.len());
                groups.push((wallet, vec![id]));
            }
        }
    }
    groups
}

/// What one wallet transaction does.
#[derive(Debug, Clone, Copy)]
enum WalletJob<'a> {
    /// Expire these due reservations.
    Reservations(&'a [Uuid]),
    /// Expire every due bucket.
    Buckets,
}

impl WalletJob<'_> {
    fn name(self) -> &'static str {
        match self {
            Self::Reservations(_) => "reservations",
            Self::Buckets => "buckets",
        }
    }
}

enum WalletOutcome {
    /// Committed; how many reservations or buckets were expired.
    Done(usize),
    /// Another transaction holds the wallet.
    Busy,
    /// The work failed; it was rolled back and logged.
    Failed,
}

/// Run `job` for one wallet in a transaction of its own. Errors of this
/// wallet's own come back as [`WalletOutcome::Failed`]; `Err` means the
/// database could not be used at all.
async fn reap_wallet(pool: &PgPool, wallet: &WalletKey, job: WalletJob<'_>) -> Result<WalletOutcome, ApiError> {
    let mut tx = pool.begin().await?;
    let result = match job {
        WalletJob::Reservations(ids) => expire_wallet_reservations(&mut tx, wallet, ids).await,
        WalletJob::Buckets => expire_wallet_buckets(&mut tx, wallet).await,
    };
    let error = match result {
        Ok(Some(expired)) => match tx.commit().await {
            Ok(()) => return Ok(WalletOutcome::Done(expired)),
            // The server refused the commit and rolled it back; the
            // connection still works.
            Err(err @ sqlx::Error::Database(_)) => ApiError::from(err),
            Err(err) => return Err(err.into()),
        },
        Ok(None) => {
            tx.rollback().await?;
            return Ok(WalletOutcome::Busy);
        }
        Err(err) => {
            // If the database accepts the rollback, the connection is fine and
            // the failure belongs to this wallet. If not, end the pass.
            if let Err(rollback) = tx.rollback().await {
                return Err(ApiError::internal(format!("{err}; rollback failed: {rollback}")));
            }
            err
        }
    };
    let (account_id, tenant_id) = wallet;
    tracing::error!(
        %account_id,
        %tenant_id,
        job = job.name(),
        error = %error,
        "reaper could not process wallet; rolled back, will retry"
    );
    Ok(WalletOutcome::Failed)
}

/// Expire the wallet's reservations among `ids` that are still PENDING.
/// Returns how many were expired, or `None` if the wallet is busy.
async fn expire_wallet_reservations(
    conn: &mut PgConnection,
    (account_id, tenant_id): &WalletKey,
    ids: &[Uuid],
) -> Result<Option<usize>, ApiError> {
    let Some(locked) = ledger::try_lock_wallet(conn, account_id, tenant_id).await? else {
        return Ok(None);
    };
    let pending = sqlx::query_as::<_, Reservation>(&format!(
        "SELECT {RESERVATION_COLUMNS} FROM reservations \
         WHERE id = ANY($1) AND account_id = $2 AND tenant_id = $3 AND status = 'PENDING' \
         ORDER BY expires_at, id"
    ))
    .bind(ids)
    .bind(account_id)
    .bind(tenant_id)
    .fetch_all(&mut *conn)
    .await?;
    if pending.is_empty() {
        return Ok(Some(0));
    }
    let mut op = Op::begin(conn, locked, "reaper").await?;
    for reservation in &pending {
        ledger::expire_reservation(&mut op, reservation, "reaper")?;
        op.flush(conn).await?;
    }
    Ok(Some(pending.len()))
}

/// Expire the wallet's due buckets. Returns how many still had credits, or
/// `None` if the wallet is busy.
async fn expire_wallet_buckets(
    conn: &mut PgConnection,
    (account_id, tenant_id): &WalletKey,
) -> Result<Option<usize>, ApiError> {
    let Some(locked) = ledger::try_lock_wallet(conn, account_id, tenant_id).await? else {
        return Ok(None);
    };
    let mut op = Op::new(locked);
    let expired = op.expire_due_buckets(conn, "reaper").await?;
    op.flush(conn).await?;
    Ok(Some(expired))
}
