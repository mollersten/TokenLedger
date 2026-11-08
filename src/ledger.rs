//! Transactional core.
//!
//! Every mutation runs in one transaction that holds the tenant's wallet row
//! lock, and keeps the number of database round trips made *while holding
//! that lock* to a minimum, because that time caps per-tenant throughput:
//!
//! 1. `SELECT ... FROM wallets ... FOR UPDATE` (also returns `NOW()`)
//! 2. commit/release only: read the reservation. This must be a separate
//!    statement so its snapshot is taken *after* the lock is granted.
//! 3. One data-modifying statement with every write ([`WriteSet::flush`]):
//!    wallet balances, reservation insert/update, all ledger entries and the
//!    idempotency record.
//! 4. `COMMIT`
//!
//! Balance arithmetic happens in Rust on the locked row; the resulting
//! balances are written back as explicit values.
//!
//! Reservation rows are only ever modified by a transaction that holds their
//! wallet's lock (handlers always; the reaper only after `try_lock_wallet`
//! succeeds), so reading a reservation without `FOR UPDATE` after the wallet
//! lock is granted always sees its latest state, and the write statement's
//! `UPDATE ... WHERE status = 'PENDING'` is a backstop that must match.
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
//!
//! So total usage for a reservation = -(COMMIT.delta_reserved + OVERAGE_DEDUCT.delta_available).

use axum::http::StatusCode;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde_json::{Map, Value, json};
use sqlx::{PgConnection, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use crate::{
    error::ApiError,
    models::{
        CommitRequest, EntryType, GrantRequest, LEDGER_COLUMNS, LedgerEntry, Outcome,
        RESERVATION_COLUMNS, ReleaseRequest, Reservation, ReservationStatus, ReserveRequest,
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
) -> Result<Option<Wallet>, sqlx::Error> {
    sqlx::query_as::<_, Wallet>(&format!(
        "SELECT {WALLET_COLUMNS} FROM wallets \
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

// ---------------------------------------------------------------------------
// WriteSet: every write of one operation, flushed as a single statement
// ---------------------------------------------------------------------------

struct Movement {
    entry_type: EntryType,
    delta_available: i64,
    delta_reserved: i64,
    reservation_id: Option<Uuid>,
    metadata: Value,
}

#[derive(Debug)]
struct LedgerRow {
    id: Uuid,
    reservation_id: Option<Uuid>,
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
}

#[derive(Debug)]
struct ReservationUpdate {
    id: Uuid,
    status: ReservationStatus,
    committed_amount: Option<i64>,
    merge_metadata: Value,
}

#[derive(Debug)]
struct IdempotencyRecord {
    key: String,
    endpoint: &'static str,
    request_hash: String,
    status: i16,
    body: Value,
}

/// Pending writes for one tenant. Built in memory while the wallet is
/// locked, then applied by [`WriteSet::flush`] in one round trip.
#[derive(Debug)]
pub struct WriteSet {
    account_id: String,
    tenant_id: String,
    wallet: Option<(i64, i64)>,
    new_reservation: Option<NewReservation>,
    reservation_update: Option<ReservationUpdate>,
    ledger: Vec<LedgerRow>,
    idempotency: Option<IdempotencyRecord>,
}

impl WriteSet {
    pub fn new(account_id: &str, tenant_id: &str) -> Self {
        Self {
            account_id: account_id.to_owned(),
            tenant_id: tenant_id.to_owned(),
            wallet: None,
            new_reservation: None,
            reservation_update: None,
            ledger: Vec::new(),
            idempotency: None,
        }
    }

    /// Apply a balance movement to the in-memory wallet and queue its ledger
    /// entry. Returns the new entry's id.
    fn movement(&mut self, wallet: &mut Wallet, movement: Movement) -> Result<Uuid, ApiError> {
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
        self.ledger.push(LedgerRow {
            id,
            reservation_id: movement.reservation_id,
            entry_type: movement.entry_type,
            delta_available: movement.delta_available,
            delta_reserved: movement.delta_reserved,
            balance_available_after: available,
            balance_reserved_after: reserved,
            metadata: movement.metadata,
        });
        wallet.available_credits = available;
        wallet.reserved_credits = reserved;
        self.wallet = Some((available, reserved));
        Ok(id)
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

    /// Store `outcome` under the request's Idempotency-Key in the same
    /// statement. The insert uses `ON CONFLICT DO NOTHING` and gates every
    /// other write: if a request with the same key committed while this one
    /// waited for the wallet lock, nothing is applied.
    pub fn record_idempotency(&mut self, key: &str, endpoint: &'static str, request_hash: &str, outcome: &Outcome) {
        self.idempotency = Some(IdempotencyRecord {
            key: key.to_owned(),
            endpoint,
            request_hash: request_hash.to_owned(),
            status: outcome.status.as_u16() as i16,
            body: outcome.body.clone(),
        });
    }

    /// Apply every queued write in one statement. Returns `false` when the
    /// idempotency gate found an existing record and nothing was written.
    pub async fn flush(self, conn: &mut PgConnection) -> Result<bool, ApiError> {
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

        if let Some((available, reserved)) = self.wallet {
            qb.push(", w AS (UPDATE wallets SET available_credits = ");
            qb.push_bind(available).push(", reserved_credits = ");
            qb.push_bind(reserved).push(", updated_at = NOW() WHERE account_id = ");
            qb.push_bind(&self.account_id).push(" AND tenant_id = ");
            qb.push_bind(&self.tenant_id);
            qb.push(" AND EXISTS (SELECT 1 FROM gate) RETURNING 1)");
        }

        if let Some(r) = &self.new_reservation {
            qb.push(
                ", ri AS (INSERT INTO reservations \
                   (id, account_id, tenant_id, feature, reserved_amount, status, expires_at, metadata) \
                 SELECT ",
            );
            qb.push_bind(r.id).push(", ");
            qb.push_bind(&self.account_id).push(", ");
            qb.push_bind(&self.tenant_id).push(", ");
            qb.push_bind(&r.feature).push(", ");
            qb.push_bind(r.reserved_amount).push(", 'PENDING', ");
            qb.push_bind(r.expires_at).push(", ");
            qb.push_bind(&r.metadata);
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

        if !self.ledger.is_empty() {
            let rows = &self.ledger;
            qb.push(
                ", l AS (INSERT INTO ledger_entries \
                   (id, account_id, tenant_id, reservation_id, entry_type, delta_available, \
                    delta_reserved, balance_available_after, balance_reserved_after, metadata, created_at) \
                 SELECT t.id, ",
            );
            qb.push_bind(&self.account_id).push(", ");
            qb.push_bind(&self.tenant_id);
            // clock_timestamp() plus a per-row microsecond keeps entries from
            // one statement strictly ordered by created_at.
            qb.push(
                ", t.reservation_id, t.entry_type, t.da, t.dr, t.ba, t.br, t.metadata, \
                 clock_timestamp() + make_interval(secs => (t.ord - 1) / 1000000.0) \
                 FROM UNNEST(",
            );
            qb.push_bind(rows.iter().map(|r| r.id).collect::<Vec<_>>()).push("::uuid[], ");
            qb.push_bind(rows.iter().map(|r| r.reservation_id).collect::<Vec<_>>()).push("::uuid[], ");
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
                ") WITH ORDINALITY AS t(id, reservation_id, entry_type, da, dr, ba, br, metadata, ord) \
                 WHERE EXISTS (SELECT 1 FROM gate) RETURNING 1)",
            );
        }

        qb.push(" SELECT EXISTS (SELECT 1 FROM gate) AS applied");
        let mut expected: Vec<(&str, i64)> = Vec::new();
        if self.wallet.is_some() {
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

/// Return a PENDING reservation's hold to `available_credits` and mark it
/// EXPIRED. Used by the reaper and lazily by commit/release on overdue holds.
pub fn expire(
    writes: &mut WriteSet,
    wallet: &mut Wallet,
    reservation: &Reservation,
    source: &str,
) -> Result<(), ApiError> {
    writes.movement(
        wallet,
        Movement {
            entry_type: EntryType::Expire,
            delta_available: reservation.reserved_amount,
            delta_reserved: -reservation.reserved_amount,
            reservation_id: Some(reservation.id),
            metadata: json!({
                "source": source,
                "feature": reservation.feature,
                "expires_at": reservation.expires_at,
            }),
        },
    )?;
    writes.update_reservation(ReservationUpdate {
        id: reservation.id,
        status: ReservationStatus::Expired,
        committed_amount: None,
        merge_metadata: json!({}),
    })
}

// ---------------------------------------------------------------------------
// Operations: decide the outcome and the writes that go with it
// ---------------------------------------------------------------------------

/// The response to return (and cache) plus the writes that produce it.
pub struct Plan {
    pub outcome: Outcome,
    pub writes: WriteSet,
}

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
    writes: &mut WriteSet,
    wallet: &mut Wallet,
    now: DateTime<Utc>,
    reservation_id: Uuid,
    source: &str,
) -> Result<Result<Reservation, Outcome>, ApiError> {
    let Some(reservation) = read_reservation(conn, wallet, reservation_id).await? else {
        return Ok(Err(reservation_not_found(reservation_id)));
    };
    if reservation.status != ReservationStatus::Pending.as_str() {
        return Ok(Err(reservation_not_pending(reservation_id, &reservation.status)));
    }
    if reservation.expires_at < now {
        expire(writes, wallet, &reservation, source)?;
        return Ok(Err(reservation_not_pending(
            reservation_id,
            ReservationStatus::Expired.as_str(),
        )));
    }
    Ok(Ok(reservation))
}

pub fn grant(wallet: &mut Wallet, now: DateTime<Utc>, req: GrantRequest) -> Result<Plan, ApiError> {
    let mut writes = WriteSet::new(&wallet.account_id, &wallet.tenant_id);
    if wallet.available_credits.checked_add(req.amount).is_none() {
        let outcome = Outcome::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({
                "error": "balance_overflow",
                "message": "grant would overflow the wallet balance",
                "available_credits": wallet.available_credits,
            }),
        );
        return Ok(Plan { outcome, writes });
    }
    let entry_id = writes.movement(
        wallet,
        Movement {
            entry_type: EntryType::Grant,
            delta_available: req.amount,
            delta_reserved: 0,
            reservation_id: None,
            metadata: json!({ "reason": req.reason, "metadata": object(req.metadata) }),
        },
    )?;
    let outcome = Outcome::ok(json!({
        "tenant_id": wallet.tenant_id,
        "available_credits": wallet.available_credits,
        "reserved_credits": wallet.reserved_credits,
        "granted_credits": req.amount,
        "ledger_entry_id": entry_id,
        "updated_at": now,
    }));
    Ok(Plan { outcome, writes })
}

pub fn reserve(wallet: &mut Wallet, now: DateTime<Utc>, req: ReserveRequest) -> Result<Plan, ApiError> {
    let mut writes = WriteSet::new(&wallet.account_id, &wallet.tenant_id);
    let estimate = req.estimate_credits;
    if wallet.available_credits < estimate {
        let outcome = Outcome::new(
            StatusCode::PAYMENT_REQUIRED,
            json!({
                "allowed": false,
                "error": "insufficient_credits",
                "available_credits": wallet.available_credits,
                "requested_credits": estimate,
            }),
        );
        return Ok(Plan { outcome, writes });
    }

    let reservation_id = Uuid::new_v4();
    let ttl = req.ttl();
    let expires_at = now + ChronoDuration::seconds(ttl);
    writes.insert_reservation(NewReservation {
        id: reservation_id,
        feature: req.feature.clone(),
        reserved_amount: estimate,
        expires_at,
        metadata: object(req.metadata),
    })?;
    writes.movement(
        wallet,
        Movement {
            entry_type: EntryType::Reserve,
            delta_available: -estimate,
            delta_reserved: estimate,
            reservation_id: Some(reservation_id),
            metadata: json!({ "feature": req.feature, "ttl_seconds": ttl }),
        },
    )?;

    let outcome = Outcome::ok(json!({
        "allowed": true,
        "reservation_id": reservation_id,
        "remaining_available": wallet.available_credits,
        "reserved_balance": wallet.reserved_credits,
        "expires_at": expires_at,
    }));
    Ok(Plan { outcome, writes })
}

pub async fn commit(
    conn: &mut PgConnection,
    wallet: &mut Wallet,
    now: DateTime<Utc>,
    req: CommitRequest,
) -> Result<Plan, ApiError> {
    let mut writes = WriteSet::new(&wallet.account_id, &wallet.tenant_id);
    let reservation =
        match pending_reservation(conn, &mut writes, wallet, now, req.reservation_id, "commit").await? {
            Ok(r) => r,
            Err(outcome) => return Ok(Plan { outcome, writes }),
        };

    let held = reservation.reserved_amount;
    let actual = req.actual_credits_used;
    let consumed_from_hold = actual.min(held);
    let refund = held - consumed_from_hold;
    let overage = actual - consumed_from_hold;

    if overage > wallet.available_credits {
        let outcome = Outcome::new(
            StatusCode::PAYMENT_REQUIRED,
            json!({
                "allowed": false,
                "error": "insufficient_credits",
                "reservation_id": reservation.id,
                "available_credits": wallet.available_credits,
                "overage_credits": overage,
                "message": "actual usage exceeds the reservation and the remaining balance cannot cover the difference",
            }),
        );
        return Ok(Plan { outcome, writes });
    }

    let client_metadata = object(req.metadata);
    writes.movement(
        wallet,
        Movement {
            entry_type: EntryType::Commit,
            delta_available: 0,
            delta_reserved: -consumed_from_hold,
            reservation_id: Some(reservation.id),
            metadata: json!({
                "feature": reservation.feature,
                "reserved_amount": held,
                "actual_credits_used": actual,
                "metadata": client_metadata,
            }),
        },
    )?;
    if refund > 0 {
        writes.movement(
            wallet,
            Movement {
                entry_type: EntryType::RefundUnused,
                delta_available: refund,
                delta_reserved: -refund,
                reservation_id: Some(reservation.id),
                metadata: json!({ "reserved_amount": held, "actual_credits_used": actual }),
            },
        )?;
    }
    if overage > 0 {
        writes.movement(
            wallet,
            Movement {
                entry_type: EntryType::OverageDeduct,
                delta_available: -overage,
                delta_reserved: 0,
                reservation_id: Some(reservation.id),
                metadata: json!({ "reserved_amount": held, "actual_credits_used": actual }),
            },
        )?;
    }
    writes.update_reservation(ReservationUpdate {
        id: reservation.id,
        status: ReservationStatus::Committed,
        committed_amount: Some(actual),
        merge_metadata: client_metadata,
    })?;

    let outcome = Outcome::ok(json!({
        "reservation_id": reservation.id,
        "status": ReservationStatus::Committed.as_str(),
        "reserved_amount": held,
        "committed_amount": actual,
        "refunded_credits": refund,
        "overage_credits": overage,
        "available_credits": wallet.available_credits,
        "reserved_credits": wallet.reserved_credits,
    }));
    Ok(Plan { outcome, writes })
}

pub async fn release(
    conn: &mut PgConnection,
    wallet: &mut Wallet,
    now: DateTime<Utc>,
    req: ReleaseRequest,
) -> Result<Plan, ApiError> {
    let mut writes = WriteSet::new(&wallet.account_id, &wallet.tenant_id);
    let reservation =
        match pending_reservation(conn, &mut writes, wallet, now, req.reservation_id, "release").await? {
            Ok(r) => r,
            Err(outcome) => return Ok(Plan { outcome, writes }),
        };
    let held = reservation.reserved_amount;
    writes.movement(
        wallet,
        Movement {
            entry_type: EntryType::Release,
            delta_available: held,
            delta_reserved: -held,
            reservation_id: Some(reservation.id),
            metadata: json!({ "feature": reservation.feature, "reason": req.reason }),
        },
    )?;
    writes.update_reservation(ReservationUpdate {
        id: reservation.id,
        status: ReservationStatus::Released,
        committed_amount: None,
        merge_metadata: match &req.reason {
            Some(reason) => json!({ "release_reason": reason }),
            None => json!({}),
        },
    })?;

    let outcome = Outcome::ok(json!({
        "reservation_id": reservation.id,
        "status": ReservationStatus::Released.as_str(),
        "released_credits": held,
        "available_credits": wallet.available_credits,
        "reserved_credits": wallet.reserved_credits,
    }));
    Ok(Plan { outcome, writes })
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
