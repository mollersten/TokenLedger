//! Database rows, request payloads and the cached-response `Outcome` type.

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::types::Json;
use uuid::Uuid;

use crate::error::ApiError;

pub const DEFAULT_TTL_SECONDS: i64 = 60;
pub const MAX_TTL_SECONDS: i64 = 86_400;
const MAX_ID_LEN: usize = 255;
const MAX_REASON_LEN: usize = 1_024;

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct Wallet {
    pub account_id: String,
    pub tenant_id: String,
    pub available_credits: i64,
    pub reserved_credits: i64,
    /// The part of `available_credits` held in expiring buckets.
    pub expiring_credits: i64,
    /// Earliest `expires_at` among buckets with credits left.
    pub next_expiry_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl Wallet {
    /// Credits that never expire.
    pub fn permanent_credits(&self) -> i64 {
        self.available_credits - self.expiring_credits
    }
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct Reservation {
    pub id: Uuid,
    pub account_id: String,
    pub tenant_id: String,
    pub feature: String,
    pub reserved_amount: i64,
    pub committed_amount: Option<i64>,
    pub status: String,
    pub expires_at: DateTime<Utc>,
    pub metadata: Value,
    /// Which buckets funded the hold; the rest came from permanent credits.
    pub bucket_holds: Json<Vec<BucketHold>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Part of a reservation's hold taken from one credit bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketHold {
    pub bucket_id: Uuid,
    pub amount: i64,
    pub expires_at: DateTime<Utc>,
}

/// A bucket of expiring credits, as listed by the wallet endpoint.
#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct CreditBucket {
    pub id: Uuid,
    pub granted: i64,
    pub remaining: i64,
    pub expires_at: DateTime<Utc>,
    pub reason: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct LedgerEntry {
    pub id: Uuid,
    pub tenant_id: String,
    pub reservation_id: Option<Uuid>,
    pub bucket_id: Option<Uuid>,
    pub entry_type: String,
    pub delta_available: i64,
    pub delta_reserved: i64,
    pub balance_available_after: i64,
    pub balance_reserved_after: i64,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
}

pub const WALLET_COLUMNS: &str = "account_id, tenant_id, available_credits, reserved_credits, \
     expiring_credits, next_expiry_at, updated_at";
pub const RESERVATION_COLUMNS: &str = "id, account_id, tenant_id, feature, reserved_amount, \
     committed_amount, status, expires_at, metadata, bucket_holds, created_at, updated_at";
pub const LEDGER_COLUMNS: &str = "id, tenant_id, reservation_id, bucket_id, entry_type, \
     delta_available, delta_reserved, balance_available_after, balance_reserved_after, metadata, \
     created_at";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservationStatus {
    Pending,
    Committed,
    Released,
    Expired,
}

impl ReservationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Committed => "COMMITTED",
            Self::Released => "RELEASED",
            Self::Expired => "EXPIRED",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryType {
    Grant,
    Reserve,
    Commit,
    RefundUnused,
    OverageDeduct,
    Release,
    Expire,
    CreditExpire,
}

impl EntryType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Grant => "GRANT",
            Self::Reserve => "RESERVE",
            Self::Commit => "COMMIT",
            Self::RefundUnused => "REFUND_UNUSED",
            Self::OverageDeduct => "OVERAGE_DEDUCT",
            Self::Release => "RELEASE",
            Self::Expire => "EXPIRE",
            Self::CreditExpire => "CREDIT_EXPIRE",
        }
    }
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct GrantRequest {
    pub tenant_id: String,
    pub amount: i64,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub metadata: Option<Map<String, Value>>,
    /// When set, the credits go into a bucket that expires at this time
    /// (RFC 3339) instead of the permanent balance.
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReserveRequest {
    pub tenant_id: String,
    pub feature: String,
    pub estimate_credits: i64,
    #[serde(default)]
    pub ttl_seconds: Option<i64>,
    #[serde(default)]
    pub metadata: Option<Map<String, Value>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CommitRequest {
    pub tenant_id: String,
    pub reservation_id: Uuid,
    pub actual_credits_used: i64,
    #[serde(default)]
    pub metadata: Option<Map<String, Value>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReleaseRequest {
    pub tenant_id: String,
    pub reservation_id: Uuid,
    #[serde(default)]
    pub reason: Option<String>,
}

impl ReserveRequest {
    pub fn ttl(&self) -> i64 {
        self.ttl_seconds.unwrap_or(DEFAULT_TTL_SECONDS)
    }
}

pub fn validate_tenant_id(tenant_id: &str) -> Result<(), ApiError> {
    if tenant_id.trim().is_empty() || tenant_id.len() > MAX_ID_LEN {
        return Err(ApiError::bad_request(
            "invalid_tenant_id",
            format!("tenant_id must be 1-{MAX_ID_LEN} characters"),
        ));
    }
    Ok(())
}

fn validate_reason(reason: Option<&str>) -> Result<(), ApiError> {
    match reason {
        Some(r) if r.len() > MAX_REASON_LEN => Err(ApiError::bad_request(
            "invalid_reason",
            format!("reason must be at most {MAX_REASON_LEN} characters"),
        )),
        _ => Ok(()),
    }
}

impl GrantRequest {
    pub fn validate(&self) -> Result<(), ApiError> {
        validate_tenant_id(&self.tenant_id)?;
        validate_reason(self.reason.as_deref())?;
        if self.amount <= 0 {
            return Err(ApiError::bad_request("invalid_amount", "amount must be greater than 0"));
        }
        Ok(())
    }
}

impl ReserveRequest {
    pub fn validate(&self) -> Result<(), ApiError> {
        validate_tenant_id(&self.tenant_id)?;
        if self.feature.trim().is_empty() || self.feature.len() > MAX_ID_LEN {
            return Err(ApiError::bad_request(
                "invalid_feature",
                format!("feature must be 1-{MAX_ID_LEN} characters"),
            ));
        }
        if self.estimate_credits <= 0 {
            return Err(ApiError::bad_request(
                "invalid_estimate_credits",
                "estimate_credits must be greater than 0",
            ));
        }
        let ttl = self.ttl();
        if !(1..=MAX_TTL_SECONDS).contains(&ttl) {
            return Err(ApiError::bad_request(
                "invalid_ttl_seconds",
                format!("ttl_seconds must be between 1 and {MAX_TTL_SECONDS}"),
            ));
        }
        Ok(())
    }
}

impl CommitRequest {
    pub fn validate(&self) -> Result<(), ApiError> {
        validate_tenant_id(&self.tenant_id)?;
        if self.actual_credits_used < 0 {
            return Err(ApiError::bad_request(
                "invalid_actual_credits_used",
                "actual_credits_used must be 0 or greater",
            ));
        }
        Ok(())
    }
}

impl ReleaseRequest {
    pub fn validate(&self) -> Result<(), ApiError> {
        validate_tenant_id(&self.tenant_id)?;
        validate_reason(self.reason.as_deref())
    }
}

// ---------------------------------------------------------------------------
// Outcome: a business result that is persisted for idempotent replay.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Outcome {
    pub status: StatusCode,
    pub body: Value,
}

impl Outcome {
    pub fn ok(body: Value) -> Self {
        Self { status: StatusCode::OK, body }
    }

    pub fn new(status: StatusCode, body: Value) -> Self {
        Self { status, body }
    }
}
