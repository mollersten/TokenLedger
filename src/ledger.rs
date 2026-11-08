//! Transactional core. Every function here runs on a connection that is
//! already inside a transaction holding the tenant's wallet row lock
//! (`SELECT ... FOR UPDATE`), so balance arithmetic can safely be done in
//! Rust and written back with explicit values.
//!
//! Ledger entry conventions (deltas are applied to the wallet in order, and
//! each entry records the balances right after it):
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
use chrono::{DateTime, Utc};
use serde_json::{Map, Value, json};
use sqlx::PgConnection;
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
// Locking primitives
// ---------------------------------------------------------------------------

/// Create an empty wallet if the tenant has none. Concurrent callers for the
/// same new tenant block on the uncommitted insert, then see the conflict.
pub async fn ensure_wallet(conn: &mut PgConnection, tenant_id: &str) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO wallets (tenant_id) VALUES ($1) ON CONFLICT (tenant_id) DO NOTHING")
        .bind(tenant_id)
        .execute(conn)
        .await?;
    Ok(())
}

/// Pessimistic lock on the tenant's wallet row; waits for other writers.
pub async fn lock_wallet(
    conn: &mut PgConnection,
    tenant_id: &str,
) -> Result<Option<Wallet>, sqlx::Error> {
    sqlx::query_as::<_, Wallet>(&format!(
        "SELECT {WALLET_COLUMNS} FROM wallets WHERE tenant_id = $1 FOR UPDATE"
    ))
    .bind(tenant_id)
    .fetch_optional(conn)
    .await
}

/// Non-blocking variant used by the reaper: returns `None` when another
/// transaction holds the wallet, so the reaper never waits while holding
/// reservation locks (which would invert the lock order and risk deadlock).
pub async fn try_lock_wallet(
    conn: &mut PgConnection,
    tenant_id: &str,
) -> Result<Option<Wallet>, sqlx::Error> {
    sqlx::query_as::<_, Wallet>(&format!(
        "SELECT {WALLET_COLUMNS} FROM wallets WHERE tenant_id = $1 FOR UPDATE SKIP LOCKED"
    ))
    .bind(tenant_id)
    .fetch_optional(conn)
    .await
}

#[derive(Debug, sqlx::FromRow)]
struct LockedReservation {
    #[sqlx(flatten)]
    reservation: Reservation,
    overdue: bool,
}

async fn lock_reservation(
    conn: &mut PgConnection,
    tenant_id: &str,
    reservation_id: Uuid,
) -> Result<Option<LockedReservation>, sqlx::Error> {
    sqlx::query_as::<_, LockedReservation>(&format!(
        "SELECT {RESERVATION_COLUMNS}, (expires_at < NOW()) AS overdue \
         FROM reservations WHERE id = $1 AND tenant_id = $2 FOR UPDATE"
    ))
    .bind(reservation_id)
    .bind(tenant_id)
    .fetch_optional(conn)
    .await
}

// ---------------------------------------------------------------------------
// Balance movement + audit entry
// ---------------------------------------------------------------------------

struct Movement {
    entry_type: EntryType,
    delta_available: i64,
    delta_reserved: i64,
    reservation_id: Option<Uuid>,
    metadata: Value,
}

/// Apply one movement to the locked wallet and append its ledger entry.
async fn apply_movement(
    conn: &mut PgConnection,
    wallet: &mut Wallet,
    movement: Movement,
) -> Result<LedgerEntry, ApiError> {
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

    let updated_at: DateTime<Utc> = sqlx::query_scalar(
        "UPDATE wallets SET available_credits = $2, reserved_credits = $3, updated_at = NOW() \
         WHERE tenant_id = $1 RETURNING updated_at",
    )
    .bind(&wallet.tenant_id)
    .bind(available)
    .bind(reserved)
    .fetch_one(&mut *conn)
    .await?;

    // clock_timestamp() (not NOW()) keeps entries written in one transaction
    // strictly ordered by created_at.
    let entry = sqlx::query_as::<_, LedgerEntry>(&format!(
        "INSERT INTO ledger_entries \
           (id, tenant_id, reservation_id, entry_type, delta_available, delta_reserved, \
            balance_available_after, balance_reserved_after, metadata, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, clock_timestamp()) \
         RETURNING {LEDGER_COLUMNS}"
    ))
    .bind(Uuid::new_v4())
    .bind(&wallet.tenant_id)
    .bind(movement.reservation_id)
    .bind(movement.entry_type.as_str())
    .bind(movement.delta_available)
    .bind(movement.delta_reserved)
    .bind(available)
    .bind(reserved)
    .bind(&movement.metadata)
    .fetch_one(&mut *conn)
    .await?;

    wallet.available_credits = available;
    wallet.reserved_credits = reserved;
    wallet.updated_at = updated_at;
    Ok(entry)
}

async fn set_reservation_status(
    conn: &mut PgConnection,
    reservation_id: Uuid,
    status: ReservationStatus,
    committed_amount: Option<i64>,
    merge_metadata: Option<&Value>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE reservations \
         SET status = $2, \
             committed_amount = COALESCE($3, committed_amount), \
             metadata = metadata || COALESCE($4, '{}'::jsonb), \
             updated_at = NOW() \
         WHERE id = $1",
    )
    .bind(reservation_id)
    .bind(status.as_str())
    .bind(committed_amount)
    .bind(merge_metadata)
    .execute(conn)
    .await?;
    Ok(())
}

/// Return a PENDING reservation's hold to `available_credits` and mark it
/// EXPIRED. Used by the reaper and lazily by commit/release on overdue holds.
pub async fn expire_reservation(
    conn: &mut PgConnection,
    wallet: &mut Wallet,
    reservation: &Reservation,
    source: &str,
) -> Result<LedgerEntry, ApiError> {
    let entry = apply_movement(
        conn,
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
    )
    .await?;
    set_reservation_status(conn, reservation.id, ReservationStatus::Expired, None, None).await?;
    Ok(entry)
}

// ---------------------------------------------------------------------------
// Operations (each returns a cacheable business outcome)
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
    wallet: &mut Wallet,
    reservation_id: Uuid,
    source: &str,
) -> Result<Result<Reservation, Outcome>, ApiError> {
    let Some(locked) = lock_reservation(conn, &wallet.tenant_id, reservation_id).await? else {
        return Ok(Err(reservation_not_found(reservation_id)));
    };
    let reservation = locked.reservation;
    if reservation.status != ReservationStatus::Pending.as_str() {
        return Ok(Err(reservation_not_pending(reservation_id, &reservation.status)));
    }
    if locked.overdue {
        expire_reservation(conn, wallet, &reservation, source).await?;
        return Ok(Err(reservation_not_pending(
            reservation_id,
            ReservationStatus::Expired.as_str(),
        )));
    }
    Ok(Ok(reservation))
}

pub async fn grant(
    conn: &mut PgConnection,
    wallet: &mut Wallet,
    req: GrantRequest,
) -> Result<Outcome, ApiError> {
    if wallet.available_credits.checked_add(req.amount).is_none() {
        return Ok(Outcome::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({
                "error": "balance_overflow",
                "message": "grant would overflow the wallet balance",
                "available_credits": wallet.available_credits,
            }),
        ));
    }
    let entry = apply_movement(
        conn,
        wallet,
        Movement {
            entry_type: EntryType::Grant,
            delta_available: req.amount,
            delta_reserved: 0,
            reservation_id: None,
            metadata: json!({ "reason": req.reason, "metadata": object(req.metadata) }),
        },
    )
    .await?;
    Ok(Outcome::ok(json!({
        "tenant_id": wallet.tenant_id,
        "available_credits": wallet.available_credits,
        "reserved_credits": wallet.reserved_credits,
        "granted_credits": req.amount,
        "ledger_entry_id": entry.id,
        "updated_at": wallet.updated_at,
    })))
}

pub async fn reserve(
    conn: &mut PgConnection,
    wallet: &mut Wallet,
    req: ReserveRequest,
) -> Result<Outcome, ApiError> {
    let estimate = req.estimate_credits;
    if wallet.available_credits < estimate {
        return Ok(Outcome::new(
            StatusCode::PAYMENT_REQUIRED,
            json!({
                "allowed": false,
                "error": "insufficient_credits",
                "available_credits": wallet.available_credits,
                "requested_credits": estimate,
            }),
        ));
    }

    let reservation_id = Uuid::new_v4();
    let metadata = object(req.metadata.clone());
    let expires_at: DateTime<Utc> = sqlx::query_scalar(
        "INSERT INTO reservations \
           (id, tenant_id, feature, reserved_amount, status, expires_at, metadata) \
         VALUES ($1, $2, $3, $4, 'PENDING', NOW() + make_interval(secs => $5::float8), $6) \
         RETURNING expires_at",
    )
    .bind(reservation_id)
    .bind(&wallet.tenant_id)
    .bind(&req.feature)
    .bind(estimate)
    .bind(req.ttl())
    .bind(&metadata)
    .fetch_one(&mut *conn)
    .await?;

    apply_movement(
        conn,
        wallet,
        Movement {
            entry_type: EntryType::Reserve,
            delta_available: -estimate,
            delta_reserved: estimate,
            reservation_id: Some(reservation_id),
            metadata: json!({ "feature": req.feature, "ttl_seconds": req.ttl() }),
        },
    )
    .await?;

    Ok(Outcome::ok(json!({
        "allowed": true,
        "reservation_id": reservation_id,
        "remaining_available": wallet.available_credits,
        "reserved_balance": wallet.reserved_credits,
        "expires_at": expires_at,
    })))
}

pub async fn commit(
    conn: &mut PgConnection,
    wallet: &mut Wallet,
    req: CommitRequest,
) -> Result<Outcome, ApiError> {
    let reservation = match pending_reservation(conn, wallet, req.reservation_id, "commit").await? {
        Ok(r) => r,
        Err(outcome) => return Ok(outcome),
    };

    let held = reservation.reserved_amount;
    let actual = req.actual_credits_used;
    let consumed_from_hold = actual.min(held);
    let refund = held - consumed_from_hold;
    let overage = actual - consumed_from_hold;

    if overage > wallet.available_credits {
        return Ok(Outcome::new(
            StatusCode::PAYMENT_REQUIRED,
            json!({
                "allowed": false,
                "error": "insufficient_credits",
                "reservation_id": reservation.id,
                "available_credits": wallet.available_credits,
                "overage_credits": overage,
                "message": "actual usage exceeds the reservation and the remaining balance cannot cover the difference",
            }),
        ));
    }

    let client_metadata = object(req.metadata);
    apply_movement(
        conn,
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
    )
    .await?;
    if refund > 0 {
        apply_movement(
            conn,
            wallet,
            Movement {
                entry_type: EntryType::RefundUnused,
                delta_available: refund,
                delta_reserved: -refund,
                reservation_id: Some(reservation.id),
                metadata: json!({ "reserved_amount": held, "actual_credits_used": actual }),
            },
        )
        .await?;
    }
    if overage > 0 {
        apply_movement(
            conn,
            wallet,
            Movement {
                entry_type: EntryType::OverageDeduct,
                delta_available: -overage,
                delta_reserved: 0,
                reservation_id: Some(reservation.id),
                metadata: json!({ "reserved_amount": held, "actual_credits_used": actual }),
            },
        )
        .await?;
    }
    set_reservation_status(
        conn,
        reservation.id,
        ReservationStatus::Committed,
        Some(actual),
        Some(&client_metadata),
    )
    .await?;

    Ok(Outcome::ok(json!({
        "reservation_id": reservation.id,
        "status": ReservationStatus::Committed.as_str(),
        "reserved_amount": held,
        "committed_amount": actual,
        "refunded_credits": refund,
        "overage_credits": overage,
        "available_credits": wallet.available_credits,
        "reserved_credits": wallet.reserved_credits,
    })))
}

pub async fn release(
    conn: &mut PgConnection,
    wallet: &mut Wallet,
    req: ReleaseRequest,
) -> Result<Outcome, ApiError> {
    let reservation = match pending_reservation(conn, wallet, req.reservation_id, "release").await? {
        Ok(r) => r,
        Err(outcome) => return Ok(outcome),
    };
    let held = reservation.reserved_amount;
    apply_movement(
        conn,
        wallet,
        Movement {
            entry_type: EntryType::Release,
            delta_available: held,
            delta_reserved: -held,
            reservation_id: Some(reservation.id),
            metadata: json!({ "feature": reservation.feature, "reason": req.reason }),
        },
    )
    .await?;
    let reason_metadata = req.reason.as_ref().map(|r| json!({ "release_reason": r }));
    set_reservation_status(
        conn,
        reservation.id,
        ReservationStatus::Released,
        None,
        reason_metadata.as_ref(),
    )
    .await?;

    Ok(Outcome::ok(json!({
        "reservation_id": reservation.id,
        "status": ReservationStatus::Released.as_str(),
        "released_credits": held,
        "available_credits": wallet.available_credits,
        "reserved_credits": wallet.reserved_credits,
    })))
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

pub async fn fetch_wallet(
    conn: &mut PgConnection,
    tenant_id: &str,
) -> Result<Option<Wallet>, sqlx::Error> {
    sqlx::query_as::<_, Wallet>(&format!("SELECT {WALLET_COLUMNS} FROM wallets WHERE tenant_id = $1"))
        .bind(tenant_id)
        .fetch_optional(conn)
        .await
}

pub async fn recent_ledger(
    conn: &mut PgConnection,
    tenant_id: &str,
    limit: i64,
) -> Result<Vec<LedgerEntry>, sqlx::Error> {
    sqlx::query_as::<_, LedgerEntry>(&format!(
        "SELECT {LEDGER_COLUMNS} FROM ledger_entries WHERE tenant_id = $1 \
         ORDER BY created_at DESC, id DESC LIMIT $2"
    ))
    .bind(tenant_id)
    .bind(limit)
    .fetch_all(conn)
    .await
}
