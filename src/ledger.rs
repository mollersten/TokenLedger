//! Transactional core.
//!
//! Every mutation runs in one transaction that holds the tenant's wallet row
//! lock, and keeps the number of database round trips made *while holding
//! that lock* to a minimum, because that time caps per-tenant throughput:
//!
//! 1. `SELECT ... FROM wallets ... FOR UPDATE` (also returns `NOW()`)
//! 2. Only when needed, read what the operation depends on. Each read is a
//!    separate statement so its snapshot is taken *after* the lock is granted:
//!    - commit/release: the reservation;
//!    - the wallet's live credit buckets, when a bucket is due to expire or
//!      when credits are spent from a wallet that has expiring credits.
//! 3. One data-modifying statement with every write ([`WriteSet::flush`]):
//!    wallet balances, reservation insert/update, credit bucket
//!    insert/updates, all ledger entries and the idempotency record.
//! 4. `COMMIT`
//!
//! Balance arithmetic happens in Rust on the locked row; the resulting
//! balances are written back as explicit values.
//!
//! Reservation and bucket rows are only ever modified by a transaction that
//! holds their wallet's lock (handlers always; the reaper only after
//! `try_lock_wallet` succeeds), so reading them without `FOR UPDATE` after
//! the wallet lock is granted always sees their latest state, and the write
//! statement's `UPDATE ... WHERE status = 'PENDING'` is a backstop that must
//! match.
//!
//! # Expiring credits
//!
//! `available_credits` = permanent credits + `expiring_credits`, where
//! expiring credits live in buckets (one per grant with `expires_at`).
//! Spending (reserve, overage) takes from buckets first, soonest expiry
//! first, then from permanent credits. A reservation remembers which buckets
//! funded it (`bucket_holds`); unused credits go back to those buckets, and
//! a part whose bucket expired in the meantime is expired on return. Due
//! buckets are expired by the reaper, and also by any request that locks the
//! wallet after `next_expiry_at`, so expired credits can never be spent.
//!
//! Ledger entry conventions (deltas apply to the wallet in order; each entry
//! records the balances right after it):
//!
//! | entry           | delta_available | delta_reserved      |
//! |-----------------|-----------------|---------------------|
//! | GRANT           | +amount         | 0                   |
//! | RESERVE         | -estimate       | +estimate           |
//! | COMMIT          | 0               | -min(actual, held)  |  (credits consumed from the hold)
//! | REFUND_UNUSED   | +unused         | -unused             |  (held - actual, when > 0)
//! | OVERAGE_DEDUCT  | -overage        | 0                   |  (actual - held, when > 0)
//! | RELEASE/EXPIRE  | +held           | -held               |
//! | CREDIT_EXPIRE   | -expired        | 0                   |  (bucket_id set)
//!
//! So total usage for a reservation = -(COMMIT.delta_reserved + OVERAGE_DEDUCT.delta_available).

use std::collections::BTreeMap;

use axum::http::StatusCode;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde_json::{Map, Value, json};
use sqlx::{PgConnection, Postgres, QueryBuilder, Row, types::Json};
use uuid::Uuid;

use crate::{
    error::ApiError,
    models::{
        BucketHold, CommitRequest, CreditBucket, EntryType, GrantRequest, LEDGER_COLUMNS, LedgerEntry,
        Outcome, RESERVATION_COLUMNS, ReleaseRequest, Reservation, ReservationStatus, ReserveRequest,
        WALLET_COLUMNS, Wallet,
    },
};

// ---------------------------------------------------------------------------
// Locking
// ---------------------------------------------------------------------------

/// A wallet row locked `FOR UPDATE`, plus the transaction timestamp (`NOW()`),
/// which every timestamp written by this transaction is derived from.
#[derive(Debug, sqlx::FromRow)]
pub struct LockedWallet {
    #[sqlx(flatten)]
    pub wallet: Wallet,
    pub now: DateTime<Utc>,
}

/// Pessimistic lock on the tenant's wallet row; waits for other writers.
pub async fn lock_wallet(
    conn: &mut PgConnection,
    account_id: &str,
    tenant_id: &str,
) -> Result<Option<LockedWallet>, sqlx::Error> {
    sqlx::query_as::<_, LockedWallet>(&format!(
        "SELECT {WALLET_COLUMNS}, NOW() AS now FROM wallets \
         WHERE account_id = $1 AND tenant_id = $2 FOR UPDATE"
    ))
    .bind(account_id)
    .bind(tenant_id)
    .fetch_optional(conn)
    .await
}

/// Lock the wallet, creating it first if the tenant has none (grant only).
///
/// For accounts with a tenant limit, creating a wallet takes
/// `FOR NO KEY UPDATE` on the account row, so concurrent creations for one
/// account serialise and the `max_tenants` check cannot be raced past. (That
/// lock mode does not conflict with the key-share locks foreign keys take.)
/// Unlimited accounts skip it.
pub async fn lock_or_create_wallet(
    conn: &mut PgConnection,
    account_id: &str,
    tenant_id: &str,
    max_tenants: Option<i32>,
) -> Result<LockedWallet, ApiError> {
    if let Some(locked) = lock_wallet(conn, account_id, tenant_id).await? {
        return Ok(locked);
    }
    if let Some(max) = max_tenants {
        sqlx::query("SELECT 1 FROM accounts WHERE id = $1 FOR NO KEY UPDATE")
            .bind(account_id)
            .execute(&mut *conn)
            .await?;
        let tenants: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM wallets WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(&mut *conn)
            .await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM wallets WHERE account_id = $1 AND tenant_id = $2)",
        )
        .bind(account_id)
        .bind(tenant_id)
        .fetch_one(&mut *conn)
        .await?;
        if !exists && tenants >= i64::from(max) {
            return Err(ApiError::forbidden(
                "tenant_limit_reached",
                format!("this account is limited to {max} tenants"),
            ));
        }
    }
    sqlx::query(
        "INSERT INTO wallets (account_id, tenant_id) VALUES ($1, $2) \
         ON CONFLICT (account_id, tenant_id) DO NOTHING",
    )
    .bind(account_id)
    .bind(tenant_id)
    .execute(&mut *conn)
    .await?;
    Ok(lock_wallet(conn, account_id, tenant_id).await?.ok_or(sqlx::Error::RowNotFound)?)
}

/// Non-blocking variant used by the reaper: returns `None` when another
/// transaction holds the wallet, so the reaper never waits while holding
/// reservation locks (which would invert the lock order and risk deadlock).
pub async fn try_lock_wallet(
    conn: &mut PgConnection,
    account_id: &str,
    tenant_id: &str,
) -> Result<Option<LockedWallet>, sqlx::Error> {
    sqlx::query_as::<_, LockedWallet>(&format!(
        "SELECT {WALLET_COLUMNS}, NOW() AS now FROM wallets \
         WHERE account_id = $1 AND tenant_id = $2 FOR UPDATE SKIP LOCKED"
    ))
    .bind(account_id)
    .bind(tenant_id)
    .fetch_optional(conn)
    .await
}

async fn read_reservation(
    conn: &mut PgConnection,
    wallet: &Wallet,
    reservation_id: Uuid,
) -> Result<Option<Reservation>, sqlx::Error> {
    sqlx::query_as::<_, Reservation>(&format!(
        "SELECT {RESERVATION_COLUMNS} FROM reservations \
         WHERE id = $1 AND account_id = $2 AND tenant_id = $3"
    ))
    .bind(reservation_id)
    .bind(&wallet.account_id)
    .bind(&wallet.tenant_id)
    .fetch_optional(conn)
    .await
}

/// A bucket with credits left, in spend order.
#[derive(Debug, Clone, sqlx::FromRow)]
struct LiveBucket {
    id: Uuid,
    remaining: i64,
    expires_at: DateTime<Utc>,
}

async fn read_live_buckets(conn: &mut PgConnection, wallet: &Wallet) -> Result<Vec<LiveBucket>, sqlx::Error> {
    sqlx::query_as::<_, LiveBucket>(
        "SELECT id, remaining, expires_at FROM credit_buckets \
         WHERE account_id = $1 AND tenant_id = $2 AND settled_at IS NULL AND remaining > 0 \
         ORDER BY expires_at, id",
    )
    .bind(&wallet.account_id)
    .bind(&wallet.tenant_id)
    .fetch_all(conn)
    .await
}

// ---------------------------------------------------------------------------
// WriteSet: every write of one operation, flushed as a single statement
// ---------------------------------------------------------------------------

struct Movement {
    entry_type: EntryType,
    delta_available: i64,
    delta_reserved: i64,
    reservation_id: Option<Uuid>,
    bucket_id: Option<Uuid>,
    metadata: Value,
}

#[derive(Debug)]
struct LedgerRow {
    id: Uuid,
    reservation_id: Option<Uuid>,
    bucket_id: Option<Uuid>,
    entry_type: EntryType,
    delta_available: i64,
    delta_reserved: i64,
    balance_available_after: i64,
    balance_reserved_after: i64,
    metadata: Value,
}

#[derive(Debug)]
struct NewReservation {
    id: Uuid,
    feature: String,
    reserved_amount: i64,
    expires_at: DateTime<Utc>,
    metadata: Value,
    bucket_holds: Vec<BucketHold>,
}

#[derive(Debug)]
struct ReservationUpdate {
    id: Uuid,
    status: ReservationStatus,
    committed_amount: Option<i64>,
    merge_metadata: Value,
}

#[derive(Debug)]
struct NewBucket {
    id: Uuid,
    granted: i64,
    expires_at: DateTime<Utc>,
    reason: Option<String>,
}

#[derive(Debug)]
struct IdempotencyRecord {
    key: String,
    endpoint: &'static str,
    request_hash: String,
    status: i16,
    body: Value,
}

/// Pending writes for one tenant, applied by [`WriteSet::flush`] in one
/// round trip. Built through [`Op`].
#[derive(Debug)]
struct WriteSet {
    account_id: String,
    tenant_id: String,
    wallet_changed: bool,
    new_reservation: Option<NewReservation>,
    reservation_update: Option<ReservationUpdate>,
    new_bucket: Option<NewBucket>,
    /// Net change to each existing bucket's `remaining`.
    bucket_deltas: BTreeMap<Uuid, i64>,
    /// Mark every bucket past its expiry as settled (see [`Op::expire_due_buckets`]).
    settle_due_buckets: bool,
    ledger: Vec<LedgerRow>,
    idempotency: Option<IdempotencyRecord>,
}

impl WriteSet {
    fn new(account_id: &str, tenant_id: &str) -> Self {
        Self {
            account_id: account_id.to_owned(),
            tenant_id: tenant_id.to_owned(),
            wallet_changed: false,
            new_reservation: None,
            reservation_update: None,
            new_bucket: None,
            bucket_deltas: BTreeMap::new(),
            settle_due_buckets: false,
            ledger: Vec::new(),
            idempotency: None,
        }
    }

    fn insert_reservation(&mut self, reservation: NewReservation) -> Result<(), ApiError> {
        if self.new_reservation.is_some() || self.reservation_update.is_some() {
            return Err(ApiError::internal("one reservation write per operation"));
        }
        self.new_reservation = Some(reservation);
        Ok(())
    }

    fn update_reservation(&mut self, update: ReservationUpdate) -> Result<(), ApiError> {
        if self.new_reservation.is_some() || self.reservation_update.is_some() {
            return Err(ApiError::internal("one reservation write per operation"));
        }
        self.reservation_update = Some(update);
        Ok(())
    }

    /// Apply every queued write in one statement, writing `wallet`'s balances
    /// if anything changed them. Returns `false` when the idempotency gate
    /// found an existing record and nothing was written.
    async fn flush(self, conn: &mut PgConnection, wallet: &Wallet) -> Result<bool, ApiError> {
        if self.wallet_changed
            && (wallet.reserved_credits < 0
                || wallet.expiring_credits < 0
                || wallet.expiring_credits > wallet.available_credits)
        {
            // The CHECK constraints are the backstop; fail with a clear message first.
            return Err(ApiError::internal(format!(
                "refusing to write inconsistent balances for tenant {}: available {}, reserved {}, expiring {}",
                self.tenant_id, wallet.available_credits, wallet.reserved_credits, wallet.expiring_credits
            )));
        }
        let bucket_deltas: Vec<(Uuid, i64)> =
            self.bucket_deltas.iter().filter(|(_, d)| **d != 0).map(|(id, d)| (*id, *d)).collect();

        let mut qb = QueryBuilder::<Postgres>::new("WITH gate AS (");
        match &self.idempotency {
            Some(idem) => {
                qb.push(
                    "INSERT INTO idempotency_keys \
                       (account_id, tenant_id, idempotency_key, endpoint, request_hash, \
                        response_status, response_body) \
                     VALUES (",
                );
                qb.push_bind(&self.account_id).push(", ");
                qb.push_bind(&self.tenant_id).push(", ");
                qb.push_bind(&idem.key).push(", ");
                qb.push_bind(idem.endpoint).push(", ");
                qb.push_bind(&idem.request_hash).push(", ");
                qb.push_bind(idem.status).push(", ");
                qb.push_bind(&idem.body);
                qb.push(") ON CONFLICT (account_id, tenant_id, idempotency_key) DO NOTHING RETURNING 1");
            }
            None => {
                qb.push("SELECT 1");
            }
        }
        qb.push(")");

        if self.wallet_changed {
            qb.push(", w AS (UPDATE wallets SET available_credits = ");
            qb.push_bind(wallet.available_credits).push(", reserved_credits = ");
            qb.push_bind(wallet.reserved_credits).push(", expiring_credits = ");
            qb.push_bind(wallet.expiring_credits).push(", next_expiry_at = ");
            qb.push_bind(wallet.next_expiry_at).push(", updated_at = NOW() WHERE account_id = ");
            qb.push_bind(&self.account_id).push(" AND tenant_id = ");
            qb.push_bind(&self.tenant_id);
            qb.push(" AND EXISTS (SELECT 1 FROM gate) RETURNING 1)");
        }

        if let Some(r) = &self.new_reservation {
            qb.push(
                ", ri AS (INSERT INTO reservations \
                   (id, account_id, tenant_id, feature, reserved_amount, status, expires_at, metadata, \
                    bucket_holds) \
                 SELECT ",
            );
            qb.push_bind(r.id).push(", ");
            qb.push_bind(&self.account_id).push(", ");
            qb.push_bind(&self.tenant_id).push(", ");
            qb.push_bind(&r.feature).push(", ");
            qb.push_bind(r.reserved_amount).push(", 'PENDING', ");
            qb.push_bind(r.expires_at).push(", ");
            qb.push_bind(&r.metadata).push(", ");
            qb.push_bind(Json(&r.bucket_holds));
            qb.push(" WHERE EXISTS (SELECT 1 FROM gate) RETURNING 1)");
        }

        if let Some(u) = &self.reservation_update {
            qb.push(", ru AS (UPDATE reservations SET status = ");
            qb.push_bind(u.status.as_str());
            qb.push(", committed_amount = COALESCE(");
            qb.push_bind(u.committed_amount);
            qb.push("::bigint, committed_amount), metadata = metadata || ");
            qb.push_bind(&u.merge_metadata);
            qb.push("::jsonb, updated_at = NOW() WHERE id = ");
            qb.push_bind(u.id);
            qb.push(" AND status = 'PENDING' AND EXISTS (SELECT 1 FROM gate) RETURNING 1)");
        }

        if let Some(b) = &self.new_bucket {
            qb.push(
                ", bi AS (INSERT INTO credit_buckets \
                   (id, account_id, tenant_id, granted, remaining, expires_at, reason) \
                 SELECT ",
            );
            qb.push_bind(b.id).push(", ");
            qb.push_bind(&self.account_id).push(", ");
            qb.push_bind(&self.tenant_id).push(", ");
            qb.push_bind(b.granted).push(", ");
            qb.push_bind(b.granted).push(", ");
            qb.push_bind(b.expires_at).push(", ");
            qb.push_bind(&b.reason);
            qb.push(" WHERE EXISTS (SELECT 1 FROM gate) RETURNING 1)");
        }

        // NOW() is this transaction's `Op::now`. A delta on a bucket past
        // that time can only be the bucket expiring (nothing is spent from or
        // returned to it), so it settles in the same update. Any other delta
        // leaves the bucket open, and re-opens it in one edge case: `now` is
        // when the transaction *began*, so a request that began just before
        // the expiry but got the wallet lock after the reaper settled the
        // bucket still returns its unused hold there. The bucket then has
        // credits again and `next_expiry_at` in the past, so the next request
        // or reaper pass expires them.
        if !bucket_deltas.is_empty() {
            qb.push(
                ", bu AS (UPDATE credit_buckets b SET remaining = b.remaining + d.delta, \
                 settled_at = CASE WHEN b.expires_at <= NOW() THEN NOW() END \
                 FROM UNNEST(",
            );
            qb.push_bind(bucket_deltas.iter().map(|(id, _)| *id).collect::<Vec<_>>()).push("::uuid[], ");
            qb.push_bind(bucket_deltas.iter().map(|(_, d)| *d).collect::<Vec<_>>()).push("::int8[]");
            qb.push(") AS d(id, delta) WHERE b.id = d.id AND b.account_id = ");
            qb.push_bind(&self.account_id).push(" AND b.tenant_id = ");
            qb.push_bind(&self.tenant_id);
            qb.push(" AND EXISTS (SELECT 1 FROM gate) RETURNING 1)");
        }

        // Due buckets that were already empty settle without a ledger entry.
        // (Disjoint from `bu`: those had credits left.)
        if self.settle_due_buckets {
            qb.push(", bs AS (UPDATE credit_buckets SET settled_at = NOW() WHERE account_id = ");
            qb.push_bind(&self.account_id).push(" AND tenant_id = ");
            qb.push_bind(&self.tenant_id);
            qb.push(
                " AND settled_at IS NULL AND expires_at <= NOW() AND remaining = 0 \
                 AND EXISTS (SELECT 1 FROM gate))",
            );
        }

        if !self.ledger.is_empty() {
            let rows = &self.ledger;
            qb.push(
                ", l AS (INSERT INTO ledger_entries \
                   (id, account_id, tenant_id, reservation_id, bucket_id, entry_type, delta_available, \
                    delta_reserved, balance_available_after, balance_reserved_after, metadata, created_at) \
                 SELECT t.id, ",
            );
            qb.push_bind(&self.account_id).push(", ");
            qb.push_bind(&self.tenant_id);
            // clock_timestamp() plus a per-row microsecond keeps entries from
            // one statement strictly ordered by created_at.
            qb.push(
                ", t.reservation_id, t.bucket_id, t.entry_type, t.da, t.dr, t.ba, t.br, t.metadata, \
                 clock_timestamp() + make_interval(secs => (t.ord - 1) / 1000000.0) \
                 FROM UNNEST(",
            );
            qb.push_bind(rows.iter().map(|r| r.id).collect::<Vec<_>>()).push("::uuid[], ");
            qb.push_bind(rows.iter().map(|r| r.reservation_id).collect::<Vec<_>>()).push("::uuid[], ");
            qb.push_bind(rows.iter().map(|r| r.bucket_id).collect::<Vec<_>>()).push("::uuid[], ");
            qb.push_bind(rows.iter().map(|r| r.entry_type.as_str().to_owned()).collect::<Vec<_>>())
                .push("::text[], ");
            qb.push_bind(rows.iter().map(|r| r.delta_available).collect::<Vec<_>>()).push("::int8[], ");
            qb.push_bind(rows.iter().map(|r| r.delta_reserved).collect::<Vec<_>>()).push("::int8[], ");
            qb.push_bind(rows.iter().map(|r| r.balance_available_after).collect::<Vec<_>>())
                .push("::int8[], ");
            qb.push_bind(rows.iter().map(|r| r.balance_reserved_after).collect::<Vec<_>>())
                .push("::int8[], ");
            qb.push_bind(rows.iter().map(|r| r.metadata.clone()).collect::<Vec<_>>()).push("::jsonb[]");
            qb.push(
                ") WITH ORDINALITY AS t(id, reservation_id, bucket_id, entry_type, da, dr, ba, br, metadata, ord) \
                 WHERE EXISTS (SELECT 1 FROM gate) RETURNING 1)",
            );
        }

        qb.push(" SELECT EXISTS (SELECT 1 FROM gate) AS applied");
        let mut expected: Vec<(&str, i64)> = Vec::new();
        if self.wallet_changed {
            qb.push(", (SELECT COUNT(*) FROM w) AS w");
            expected.push(("w", 1));
        }
        if self.new_reservation.is_some() {
            qb.push(", (SELECT COUNT(*) FROM ri) AS ri");
            expected.push(("ri", 1));
        }
        if self.reservation_update.is_some() {
            qb.push(", (SELECT COUNT(*) FROM ru) AS ru");
            expected.push(("ru", 1));
        }
        if self.new_bucket.is_some() {
            qb.push(", (SELECT COUNT(*) FROM bi) AS bi");
            expected.push(("bi", 1));
        }
        if !bucket_deltas.is_empty() {
            qb.push(", (SELECT COUNT(*) FROM bu) AS bu");
            expected.push(("bu", bucket_deltas.len() as i64));
        }
        if !self.ledger.is_empty() {
            qb.push(", (SELECT COUNT(*) FROM l) AS l");
            expected.push(("l", self.ledger.len() as i64));
        }

        let row = qb.build().fetch_one(&mut *conn).await?;
        let applied: bool = row.try_get("applied")?;
        if !applied {
            return Ok(false);
        }
        for (column, want) in expected {
            let got: i64 = row.try_get(column)?;
            if got != want {
                // e.g. the reservation was no longer PENDING. Erroring rolls
                // the whole transaction back.
                return Err(ApiError::internal(format!(
                    "write set for tenant {} applied {got} rows to {column}, expected {want}",
                    self.tenant_id
                )));
            }
        }
        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// Op: one operation on one locked wallet
// ---------------------------------------------------------------------------

/// The working state of an operation on a wallet this transaction has
/// locked: the wallet as it will be written, the writes that get it there,
/// and (once read) its live credit buckets.
#[derive(Debug)]
pub struct Op {
    pub wallet: Wallet,
    pub now: DateTime<Utc>,
    writes: WriteSet,
    /// Buckets with credits left, kept in spend order. `None` until needed.
    buckets: Option<Vec<LiveBucket>>,
}

impl Op {
    /// Start from a locked wallet without touching its buckets.
    pub fn new(locked: LockedWallet) -> Self {
        let writes = WriteSet::new(&locked.wallet.account_id, &locked.wallet.tenant_id);
        Self { wallet: locked.wallet, now: locked.now, writes, buckets: None }
    }

    /// Start an operation, first expiring any bucket that is past its expiry
    /// (only checked against `next_expiry_at`, so no extra read otherwise).
    /// Requests therefore never see or spend expired credits, however long
    /// ago the reaper last ran.
    pub async fn begin(conn: &mut PgConnection, locked: LockedWallet, source: &str) -> Result<Self, ApiError> {
        let mut op = Self::new(locked);
        if op.wallet.next_expiry_at.is_some_and(|at| at <= op.now) {
            op.expire_due_buckets(conn, source).await?;
        }
        Ok(op)
    }

    /// Store `outcome` under the request's Idempotency-Key in the same
    /// statement. The insert uses `ON CONFLICT DO NOTHING` and gates every
    /// other write: if a request with the same key committed while this one
    /// waited for the wallet lock, nothing is applied.
    pub fn record_idempotency(&mut self, key: &str, endpoint: &'static str, request_hash: &str, outcome: &Outcome) {
        self.writes.idempotency = Some(IdempotencyRecord {
            key: key.to_owned(),
            endpoint,
            request_hash: request_hash.to_owned(),
            status: outcome.status.as_u16() as i16,
            body: outcome.body.clone(),
        });
    }

    /// Write everything queued so far in one statement. The `Op` stays
    /// usable for further writes on the same locked wallet (the reaper
    /// expires several reservations per wallet). Returns `false` when the
    /// idempotency gate found an existing record and nothing was written.
    pub async fn flush(&mut self, conn: &mut PgConnection) -> Result<bool, ApiError> {
        let writes = std::mem::replace(
            &mut self.writes,
            WriteSet::new(&self.wallet.account_id, &self.wallet.tenant_id),
        );
        writes.flush(conn, &self.wallet).await
    }

    /// Apply a balance movement to the in-memory wallet and queue its ledger
    /// entry. Returns the new entry's id.
    fn movement(&mut self, movement: Movement) -> Result<Uuid, ApiError> {
        let wallet = &mut self.wallet;
        let available = wallet
            .available_credits
            .checked_add(movement.delta_available)
            .ok_or_else(|| ApiError::internal("available_credits overflow"))?;
        let reserved = wallet
            .reserved_credits
            .checked_add(movement.delta_reserved)
            .ok_or_else(|| ApiError::internal("reserved_credits overflow"))?;
        if available < 0 || reserved < 0 {
            // Callers check balances first; the CHECK constraints are the backstop.
            return Err(ApiError::internal(format!(
                "refusing {} that would make balances negative ({available}, {reserved})",
                movement.entry_type.as_str()
            )));
        }
        let id = Uuid::new_v4();
        self.writes.ledger.push(LedgerRow {
            id,
            reservation_id: movement.reservation_id,
            bucket_id: movement.bucket_id,
            entry_type: movement.entry_type,
            delta_available: movement.delta_available,
            delta_reserved: movement.delta_reserved,
            balance_available_after: available,
            balance_reserved_after: reserved,
            metadata: movement.metadata,
        });
        wallet.available_credits = available;
        wallet.reserved_credits = reserved;
        self.writes.wallet_changed = true;
        Ok(id)
    }

    /// Read the live buckets once per operation (under the wallet lock), and
    /// check they add up to the wallet's `expiring_credits`.
    async fn load_buckets(&mut self, conn: &mut PgConnection) -> Result<(), ApiError> {
        if self.buckets.is_some() {
            return Ok(());
        }
        let buckets = read_live_buckets(conn, &self.wallet).await?;
        let total: i64 = buckets.iter().map(|b| b.remaining).sum();
        if total != self.wallet.expiring_credits {
            return Err(ApiError::internal(format!(
                "credit buckets of tenant {} hold {total} credits but the wallet records {} expiring credits",
                self.wallet.tenant_id, self.wallet.expiring_credits
            )));
        }
        self.buckets = Some(buckets);
        Ok(())
    }

    /// Recompute `next_expiry_at` from the loaded buckets.
    fn refresh_next_expiry(&mut self) {
        if let Some(buckets) = &self.buckets {
            self.wallet.next_expiry_at =
                buckets.iter().filter(|b| b.remaining > 0).map(|b| b.expires_at).min();
        }
    }

    fn take_from_bucket(&mut self, id: Uuid, amount: i64) {
        if let Some(bucket) = self.buckets.as_mut().and_then(|bs| bs.iter_mut().find(|b| b.id == id)) {
            bucket.remaining -= amount;
        }
        *self.writes.bucket_deltas.entry(id).or_default() -= amount;
        self.wallet.expiring_credits -= amount;
        self.writes.wallet_changed = true;
    }

    fn put_in_bucket(&mut self, id: Uuid, amount: i64, expires_at: DateTime<Utc>) {
        if let Some(buckets) = self.buckets.as_mut() {
            match buckets.iter_mut().find(|b| b.id == id) {
                Some(bucket) => bucket.remaining += amount,
                None => {
                    buckets.push(LiveBucket { id, remaining: amount, expires_at });
                    buckets.sort_by_key(|b| (b.expires_at, b.id));
                }
            }
        }
        *self.writes.bucket_deltas.entry(id).or_default() += amount;
        self.wallet.expiring_credits += amount;
        self.wallet.next_expiry_at = Some(self.wallet.next_expiry_at.map_or(expires_at, |at| at.min(expires_at)));
        self.writes.wallet_changed = true;
    }

    /// Expire every bucket whose `expires_at` has passed: its remaining
    /// credits leave the wallet with a CREDIT_EXPIRE entry, and it is marked
    /// settled (as are due buckets that were already empty). Returns how many
    /// buckets had credits to expire.
    pub async fn expire_due_buckets(&mut self, conn: &mut PgConnection, source: &str) -> Result<usize, ApiError> {
        self.load_buckets(conn).await?;
        self.writes.settle_due_buckets = true;
        let now = self.now;
        let due: Vec<LiveBucket> = self
            .buckets
            .iter()
            .flatten()
            .filter(|b| b.remaining > 0 && b.expires_at <= now)
            .cloned()
            .collect();
        for bucket in &due {
            self.take_from_bucket(bucket.id, bucket.remaining);
            self.movement(Movement {
                entry_type: EntryType::CreditExpire,
                delta_available: -bucket.remaining,
                delta_reserved: 0,
                reservation_id: None,
                bucket_id: Some(bucket.id),
                metadata: json!({ "source": source, "expires_at": bucket.expires_at }),
            })?;
        }
        self.refresh_next_expiry();
        Ok(due.len())
    }

    /// Choose where `amount` of available credits comes from: buckets first,
    /// soonest expiry first, then permanent credits. Updates the bucket
    /// bookkeeping only; the caller queues the ledger movement (and must have
    /// checked that `amount <= available_credits`). Returns the bucket parts.
    async fn spend(&mut self, conn: &mut PgConnection, amount: i64) -> Result<Vec<BucketHold>, ApiError> {
        if self.wallet.expiring_credits == 0 {
            return Ok(Vec::new());
        }
        self.load_buckets(conn).await?;
        let mut left = amount;
        let mut parts = Vec::new();
        for bucket in self.buckets.iter().flatten() {
            if left == 0 {
                break;
            }
            let take = bucket.remaining.min(left);
            if take > 0 {
                parts.push(BucketHold { bucket_id: bucket.id, amount: take, expires_at: bucket.expires_at });
                left -= take;
            }
        }
        for part in &parts {
            self.take_from_bucket(part.bucket_id, part.amount);
        }
        self.refresh_next_expiry();
        Ok(parts)
    }

    /// Return parts of a hold to the buckets they came from, after the
    /// movement that put them back in `available_credits` has been queued.
    /// A part whose bucket has expired since is expired right away with a
    /// CREDIT_EXPIRE entry. Returns the credits expired that way.
    fn return_to_buckets(
        &mut self,
        parts: &[BucketHold],
        reservation_id: Uuid,
        source: &str,
    ) -> Result<i64, ApiError> {
        let mut expired = 0;
        for part in parts.iter().filter(|p| p.amount > 0) {
            if part.expires_at > self.now {
                self.put_in_bucket(part.bucket_id, part.amount, part.expires_at);
            } else {
                self.movement(Movement {
                    entry_type: EntryType::CreditExpire,
                    delta_available: -part.amount,
                    delta_reserved: 0,
                    reservation_id: Some(reservation_id),
                    bucket_id: Some(part.bucket_id),
                    metadata: json!({
                        "source": source,
                        "expires_at": part.expires_at,
                        "returned_from_hold": true,
                    }),
                })?;
                expired += part.amount;
            }
        }
        Ok(expired)
    }
}

/// Split a hold into the parts consumed by `consume` credits (buckets first,
/// in the order they were taken, then permanent credits) and the bucket parts
/// left over.
fn split_hold(holds: &[BucketHold], consume: i64) -> (Vec<BucketHold>, Vec<BucketHold>) {
    let mut left = consume;
    let (mut consumed, mut unused) = (Vec::new(), Vec::new());
    for hold in holds {
        let used = hold.amount.min(left);
        left -= used;
        if used > 0 {
            consumed.push(BucketHold { amount: used, ..hold.clone() });
        }
        if hold.amount > used {
            unused.push(BucketHold { amount: hold.amount - used, ..hold.clone() });
        }
    }
    (consumed, unused)
}

/// Adds `"buckets": [...]` to ledger metadata when credits moved to or from
/// buckets, so the audit trail shows which grant paid for what.
fn with_buckets(mut metadata: Value, parts: &[BucketHold]) -> Value {
    if !parts.is_empty()
        && let Some(object) = metadata.as_object_mut()
    {
        object.insert(
            "buckets".to_owned(),
            parts.iter().map(|p| json!({ "bucket_id": p.bucket_id, "amount": p.amount })).collect(),
        );
    }
    metadata
}

/// Return a PENDING reservation's hold to `available_credits` (and its
/// buckets) and mark it EXPIRED. Used by the reaper and lazily by
/// commit/release on overdue holds.
pub fn expire_reservation(op: &mut Op, reservation: &Reservation, source: &str) -> Result<(), ApiError> {
    let holds = &reservation.bucket_holds.0;
    op.movement(Movement {
        entry_type: EntryType::Expire,
        delta_available: reservation.reserved_amount,
        delta_reserved: -reservation.reserved_amount,
        reservation_id: Some(reservation.id),
        bucket_id: None,
        metadata: with_buckets(
            json!({
                "source": source,
                "feature": reservation.feature,
                "expires_at": reservation.expires_at,
            }),
            holds,
        ),
    })?;
    op.return_to_buckets(holds, reservation.id, source)?;
    op.writes.update_reservation(ReservationUpdate {
        id: reservation.id,
        status: ReservationStatus::Expired,
        committed_amount: None,
        merge_metadata: json!({}),
    })
}

// ---------------------------------------------------------------------------
// Operations: decide the outcome and queue the writes that go with it
// ---------------------------------------------------------------------------

fn object(map: Option<Map<String, Value>>) -> Value {
    Value::Object(map.unwrap_or_default())
}

fn reservation_not_found(reservation_id: Uuid) -> Outcome {
    Outcome::new(
        StatusCode::NOT_FOUND,
        json!({
            "error": "reservation_not_found",
            "reservation_id": reservation_id,
            "message": "no reservation with this id exists for this tenant",
        }),
    )
}

fn reservation_not_pending(reservation_id: Uuid, status: &str) -> Outcome {
    Outcome::new(
        StatusCode::CONFLICT,
        json!({
            "error": "reservation_not_pending",
            "reservation_id": reservation_id,
            "status": status,
            "message": format!("reservation is already {status}"),
        }),
    )
}

/// Load the reservation for commit/release. Resolves to `Err(outcome)` when
/// the request cannot proceed (not found, not pending, or past its TTL; an
/// overdue hold is expired right here so the result never depends on when
/// the reaper last ran).
async fn pending_reservation(
    conn: &mut PgConnection,
    op: &mut Op,
    reservation_id: Uuid,
    source: &str,
) -> Result<Result<Reservation, Outcome>, ApiError> {
    let Some(reservation) = read_reservation(conn, &op.wallet, reservation_id).await? else {
        return Ok(Err(reservation_not_found(reservation_id)));
    };
    if reservation.status != ReservationStatus::Pending.as_str() {
        return Ok(Err(reservation_not_pending(reservation_id, &reservation.status)));
    }
    if reservation.expires_at < op.now {
        expire_reservation(op, &reservation, source)?;
        return Ok(Err(reservation_not_pending(
            reservation_id,
            ReservationStatus::Expired.as_str(),
        )));
    }
    Ok(Ok(reservation))
}

pub fn grant(op: &mut Op, req: GrantRequest) -> Result<Outcome, ApiError> {
    if op.wallet.available_credits.checked_add(req.amount).is_none() {
        return Ok(Outcome::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({
                "error": "balance_overflow",
                "message": "grant would overflow the wallet balance",
                "available_credits": op.wallet.available_credits,
            }),
        ));
    }
    // Checked against the database clock under the lock; not cached for
    // replay (like other validation errors), and the transaction rolls back.
    if let Some(expires_at) = req.expires_at
        && expires_at <= op.now
    {
        return Err(ApiError::unprocessable("invalid_expires_at", "expires_at must be in the future"));
    }

    let bucket_id = req.expires_at.map(|_| Uuid::new_v4());
    let mut metadata = json!({ "reason": req.reason, "metadata": object(req.metadata) });
    if let Some(expires_at) = req.expires_at {
        metadata["expires_at"] = json!(expires_at);
    }
    let entry_id = op.movement(Movement {
        entry_type: EntryType::Grant,
        delta_available: req.amount,
        delta_reserved: 0,
        reservation_id: None,
        bucket_id,
        metadata,
    })?;
    if let (Some(id), Some(expires_at)) = (bucket_id, req.expires_at) {
        op.writes.new_bucket = Some(NewBucket { id, granted: req.amount, expires_at, reason: req.reason });
        if let Some(buckets) = op.buckets.as_mut() {
            buckets.push(LiveBucket { id, remaining: req.amount, expires_at });
            buckets.sort_by_key(|b| (b.expires_at, b.id));
        }
        op.wallet.expiring_credits += req.amount;
        op.wallet.next_expiry_at = Some(op.wallet.next_expiry_at.map_or(expires_at, |at| at.min(expires_at)));
    }

    let mut body = json!({
        "tenant_id": op.wallet.tenant_id,
        "available_credits": op.wallet.available_credits,
        "reserved_credits": op.wallet.reserved_credits,
        "expiring_credits": op.wallet.expiring_credits,
        "granted_credits": req.amount,
        "ledger_entry_id": entry_id,
        "updated_at": op.now,
    });
    if let (Some(id), Some(expires_at)) = (bucket_id, req.expires_at) {
        body["bucket_id"] = json!(id);
        body["expires_at"] = json!(expires_at);
    }
    Ok(Outcome::ok(body))
}

pub async fn reserve(conn: &mut PgConnection, op: &mut Op, req: ReserveRequest) -> Result<Outcome, ApiError> {
    let estimate = req.estimate_credits;
    if op.wallet.available_credits < estimate {
        return Ok(Outcome::new(
            StatusCode::PAYMENT_REQUIRED,
            json!({
                "allowed": false,
                "error": "insufficient_credits",
                "available_credits": op.wallet.available_credits,
                "requested_credits": estimate,
            }),
        ));
    }

    let reservation_id = Uuid::new_v4();
    let ttl = req.ttl();
    let expires_at = op.now + ChronoDuration::seconds(ttl);
    let holds = op.spend(conn, estimate).await?;
    op.movement(Movement {
        entry_type: EntryType::Reserve,
        delta_available: -estimate,
        delta_reserved: estimate,
        reservation_id: Some(reservation_id),
        bucket_id: None,
        metadata: with_buckets(json!({ "feature": req.feature, "ttl_seconds": ttl }), &holds),
    })?;
    op.writes.insert_reservation(NewReservation {
        id: reservation_id,
        feature: req.feature,
        reserved_amount: estimate,
        expires_at,
        metadata: object(req.metadata),
        bucket_holds: holds,
    })?;

    Ok(Outcome::ok(json!({
        "allowed": true,
        "reservation_id": reservation_id,
        "remaining_available": op.wallet.available_credits,
        "reserved_balance": op.wallet.reserved_credits,
        "expires_at": expires_at,
    })))
}

pub async fn commit(conn: &mut PgConnection, op: &mut Op, req: CommitRequest) -> Result<Outcome, ApiError> {
    let reservation = match pending_reservation(conn, op, req.reservation_id, "commit").await? {
        Ok(r) => r,
        Err(outcome) => return Ok(outcome),
    };

    let held = reservation.reserved_amount;
    let actual = req.actual_credits_used;
    let consumed_from_hold = actual.min(held);
    let refund = held - consumed_from_hold;
    let overage = actual - consumed_from_hold;

    if overage > op.wallet.available_credits {
        return Ok(Outcome::new(
            StatusCode::PAYMENT_REQUIRED,
            json!({
                "allowed": false,
                "error": "insufficient_credits",
                "reservation_id": reservation.id,
                "available_credits": op.wallet.available_credits,
                "overage_credits": overage,
                "message": "actual usage exceeds the reservation and the remaining balance cannot cover the difference",
            }),
        ));
    }

    let (consumed_parts, unused_parts) = split_hold(&reservation.bucket_holds.0, consumed_from_hold);
    let client_metadata = object(req.metadata);
    op.movement(Movement {
        entry_type: EntryType::Commit,
        delta_available: 0,
        delta_reserved: -consumed_from_hold,
        reservation_id: Some(reservation.id),
        bucket_id: None,
        metadata: with_buckets(
            json!({
                "feature": reservation.feature,
                "reserved_amount": held,
                "actual_credits_used": actual,
                "metadata": client_metadata,
            }),
            &consumed_parts,
        ),
    })?;
    let mut expired = 0;
    if refund > 0 {
        op.movement(Movement {
            entry_type: EntryType::RefundUnused,
            delta_available: refund,
            delta_reserved: -refund,
            reservation_id: Some(reservation.id),
            bucket_id: None,
            metadata: with_buckets(
                json!({ "reserved_amount": held, "actual_credits_used": actual }),
                &unused_parts,
            ),
        })?;
        expired = op.return_to_buckets(&unused_parts, reservation.id, "commit")?;
    }
    if overage > 0 {
        let parts = op.spend(conn, overage).await?;
        op.movement(Movement {
            entry_type: EntryType::OverageDeduct,
            delta_available: -overage,
            delta_reserved: 0,
            reservation_id: Some(reservation.id),
            bucket_id: None,
            metadata: with_buckets(json!({ "reserved_amount": held, "actual_credits_used": actual }), &parts),
        })?;
    }
    op.writes.update_reservation(ReservationUpdate {
        id: reservation.id,
        status: ReservationStatus::Committed,
        committed_amount: Some(actual),
        merge_metadata: client_metadata,
    })?;

    Ok(Outcome::ok(json!({
        "reservation_id": reservation.id,
        "status": ReservationStatus::Committed.as_str(),
        "reserved_amount": held,
        "committed_amount": actual,
        "refunded_credits": refund,
        "overage_credits": overage,
        "expired_credits": expired,
        "available_credits": op.wallet.available_credits,
        "reserved_credits": op.wallet.reserved_credits,
        "expiring_credits": op.wallet.expiring_credits,
    })))
}

pub async fn release(conn: &mut PgConnection, op: &mut Op, req: ReleaseRequest) -> Result<Outcome, ApiError> {
    let reservation = match pending_reservation(conn, op, req.reservation_id, "release").await? {
        Ok(r) => r,
        Err(outcome) => return Ok(outcome),
    };
    let held = reservation.reserved_amount;
    let holds = &reservation.bucket_holds.0;
    op.movement(Movement {
        entry_type: EntryType::Release,
        delta_available: held,
        delta_reserved: -held,
        reservation_id: Some(reservation.id),
        bucket_id: None,
        metadata: with_buckets(json!({ "feature": reservation.feature, "reason": req.reason }), holds),
    })?;
    let expired = op.return_to_buckets(holds, reservation.id, "release")?;
    op.writes.update_reservation(ReservationUpdate {
        id: reservation.id,
        status: ReservationStatus::Released,
        committed_amount: None,
        merge_metadata: match &req.reason {
            Some(reason) => json!({ "release_reason": reason }),
            None => json!({}),
        },
    })?;

    Ok(Outcome::ok(json!({
        "reservation_id": reservation.id,
        "status": ReservationStatus::Released.as_str(),
        "released_credits": held,
        "expired_credits": expired,
        "available_credits": op.wallet.available_credits,
        "reserved_credits": op.wallet.reserved_credits,
        "expiring_credits": op.wallet.expiring_credits,
    })))
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

pub async fn fetch_wallet(
    conn: &mut PgConnection,
    account_id: &str,
    tenant_id: &str,
) -> Result<Option<Wallet>, sqlx::Error> {
    sqlx::query_as::<_, Wallet>(&format!(
        "SELECT {WALLET_COLUMNS} FROM wallets WHERE account_id = $1 AND tenant_id = $2"
    ))
    .bind(account_id)
    .bind(tenant_id)
    .fetch_optional(conn)
    .await
}

/// A bucket listing for the wallet endpoint, with `NOW()` of the reading
/// transaction so the caller can tell which buckets are already past expiry.
#[derive(Debug, sqlx::FromRow)]
pub struct BucketRow {
    #[sqlx(flatten)]
    pub bucket: CreditBucket,
    pub due: bool,
}

/// Buckets with credits left, soonest expiry first. Includes buckets whose
/// expiry has passed but which the reaper has not processed yet (`due`).
pub async fn live_buckets(
    conn: &mut PgConnection,
    account_id: &str,
    tenant_id: &str,
) -> Result<Vec<BucketRow>, sqlx::Error> {
    sqlx::query_as::<_, BucketRow>(
        "SELECT id, granted, remaining, expires_at, reason, created_at, expires_at <= NOW() AS due \
         FROM credit_buckets \
         WHERE account_id = $1 AND tenant_id = $2 AND settled_at IS NULL AND remaining > 0 \
         ORDER BY expires_at, id",
    )
    .bind(account_id)
    .bind(tenant_id)
    .fetch_all(conn)
    .await
}

pub async fn recent_ledger(
    conn: &mut PgConnection,
    account_id: &str,
    tenant_id: &str,
    limit: i64,
) -> Result<Vec<LedgerEntry>, sqlx::Error> {
    sqlx::query_as::<_, LedgerEntry>(&format!(
        "SELECT {LEDGER_COLUMNS} FROM ledger_entries WHERE account_id = $1 AND tenant_id = $2 \
         ORDER BY created_at DESC, id DESC LIMIT $3"
    ))
    .bind(account_id)
    .bind(tenant_id)
    .bind(limit)
    .fetch_all(conn)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hold(n: u128, amount: i64) -> BucketHold {
        BucketHold { bucket_id: Uuid::from_u128(n), amount, expires_at: DateTime::<Utc>::MIN_UTC }
    }

    #[test]
    fn split_hold_consumes_buckets_in_order_then_permanent() {
        let holds = [hold(1, 4), hold(2, 3)];
        assert_eq!(split_hold(&holds, 0), (vec![], vec![hold(1, 4), hold(2, 3)]));
        assert_eq!(split_hold(&holds, 5), (vec![hold(1, 4), hold(2, 1)], vec![hold(2, 2)]));
        assert_eq!(split_hold(&holds, 7), (vec![hold(1, 4), hold(2, 3)], vec![]));
        // Beyond the bucket parts the rest of the hold is permanent credits.
        assert_eq!(split_hold(&holds, 10), (vec![hold(1, 4), hold(2, 3)], vec![]));
        assert_eq!(split_hold(&[], 10), (vec![], vec![]));
    }
}
