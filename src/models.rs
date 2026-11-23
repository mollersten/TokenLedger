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
/// Metadata is stored with the reservation (reserve) or the ledger entry
/// (grant, commit).
pub const MAX_METADATA_BYTES: usize = 16 * 1024;
/// The largest balance or amount: 2^53 - 1, the largest integer a JavaScript
/// number (and so the SDK, and most JSON parsers) holds exactly.
pub const MAX_CREDITS: i64 = 9_007_199_254_740_991;

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct Wallet {
    /// Compact numeric id, used as the key in `wallets_items`.
    pub id: i64,
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
    /// The item (`items.id`) this reservation pays for, when it is charged
    /// once per item.
    pub item_id: Option<i64>,
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
    /// The reservation's feature, for entries that belong to one.
    pub feature: Option<String>,
    /// `[{bucket_id, amount}]`: the expiring-credit buckets the credits moved
    /// from or to.
    pub buckets: Value,
    pub created_at: DateTime<Utc>,
}

pub const WALLET_COLUMNS: &str = "id, account_id, tenant_id, available_credits, reserved_credits, \
     expiring_credits, next_expiry_at, updated_at";
pub const RESERVATION_COLUMNS: &str = "id, account_id, tenant_id, feature, reserved_amount, \
     committed_amount, status, expires_at, metadata, \
     COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id', h.bucket_id, 'amount', h.amount, \
                                                   'expires_at', b.expires_at) ORDER BY b.expires_at, b.id) \
               FROM reservations_credit_buckets h JOIN credit_buckets b ON b.id = h.bucket_id \
               WHERE h.reservation_id = reservations.id), '[]'::jsonb) AS bucket_holds, \
     item_id, created_at, updated_at";
/// Ledger entry columns as `l`, with the reservation (`r`) joined for the
/// feature and the bucket parts gathered from `ledger_entries_credit_buckets`.
pub const LEDGER_COLUMNS: &str = "l.id, l.tenant_id, l.reservation_id, l.bucket_id, l.entry_type, \
     l.delta_available, l.delta_reserved, l.balance_available_after, l.balance_reserved_after, l.metadata, \
     r.feature, \
     COALESCE((SELECT jsonb_agg(jsonb_build_object('bucket_id', p.bucket_id, 'amount', p.amount) ORDER BY p.bucket_id) \
               FROM ledger_entries_credit_buckets p WHERE p.ledger_entry_id = l.id), '[]'::jsonb) AS buckets, \
     l.created_at";

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

/// Unknown fields are refused: a misspelt one (`itemKey`, `expiresAt`)
/// would otherwise be ignored and change what the request does.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// Unknown fields are refused: a misspelt one (`itemKey`, `expiresAt`)
/// would otherwise be ignored and change what the request does.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReserveRequest {
    pub tenant_id: String,
    pub feature: String,
    pub estimate_credits: i64,
    #[serde(default)]
    pub ttl_seconds: Option<i64>,
    #[serde(default)]
    pub metadata: Option<Map<String, Value>>,
    /// Charge this item once per tenant. The item is (feature, item_key): the
    /// same key under another feature is another item. While a reservation
    /// for it is pending no other can be made, and once the tenant owns it
    /// (`wallets_items`) asking again answers `already_charged` without charging. Use a new
    /// Idempotency-Key for each charge attempt (the SDK's `chargeOnce` does, and
    /// reuses it only to retry that same attempt): a key reused for a later
    /// attempt replays the first answer, not the item's current state.
    #[serde(default)]
    pub item_key: Option<String>,
}

/// Unknown fields are refused: a misspelt one (`itemKey`, `expiresAt`)
/// would otherwise be ignored and change what the request does.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitRequest {
    pub tenant_id: String,
    pub reservation_id: Uuid,
    pub actual_credits_used: i64,
    #[serde(default)]
    pub metadata: Option<Map<String, Value>>,
}

/// Unknown fields are refused: a misspelt one (`itemKey`, `expiresAt`)
/// would otherwise be ignored and change what the request does.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// Identifiers are refused with control characters: Postgres cannot store
/// a NUL byte, so letting one through would fail as a 500 instead of a 400.
pub fn validate_tenant_id(tenant_id: &str) -> Result<(), ApiError> {
    if tenant_id.trim().is_empty() || tenant_id.len() > MAX_ID_LEN || tenant_id.chars().any(char::is_control) {
        return Err(ApiError::bad_request(
            "invalid_tenant_id",
            format!("tenant_id must be 1-{MAX_ID_LEN} characters with no control characters"),
        ));
    }
    Ok(())
}

fn validate_reason(reason: Option<&str>) -> Result<(), ApiError> {
    match reason {
        Some(r) if r.len() > MAX_REASON_LEN || r.contains('\0') => Err(ApiError::bad_request(
            "invalid_reason",
            format!("reason must be at most {MAX_REASON_LEN} characters, with no NUL characters"),
        )),
        _ => Ok(()),
    }
}

impl GrantRequest {
    pub fn validate(&self) -> Result<(), ApiError> {
        validate_tenant_id(&self.tenant_id)?;
        validate_reason(self.reason.as_deref())?;
        validate_metadata(self.metadata.as_ref())?;
        validate_credits("amount", "invalid_amount", self.amount, 1)
    }
}

fn validate_metadata(metadata: Option<&Map<String, Value>>) -> Result<(), ApiError> {
    let Some(m) = metadata else { return Ok(()) };
    if serde_json::to_vec(m).map_or(usize::MAX, |b| b.len()) > MAX_METADATA_BYTES {
        return Err(ApiError::bad_request(
            "invalid_metadata",
            format!("metadata must be at most {MAX_METADATA_BYTES} bytes as JSON"),
        ));
    }
    // Postgres cannot store a NUL character in jsonb; refuse it as a 400
    // rather than fail the write with a 500.
    fn has_nul(v: &Value) -> bool {
        match v {
            Value::String(s) => s.contains('\0'),
            Value::Array(items) => items.iter().any(has_nul),
            Value::Object(map) => map.iter().any(|(k, v)| k.contains('\0') || has_nul(v)),
            _ => false,
        }
    }
    if m.iter().any(|(k, v)| k.contains('\0') || has_nul(v)) {
        return Err(ApiError::bad_request("invalid_metadata", "metadata must not contain NUL characters"));
    }
    Ok(())
}

fn validate_credits(field: &'static str, code: &'static str, value: i64, min: i64) -> Result<(), ApiError> {
    if !(min..=MAX_CREDITS).contains(&value) {
        return Err(ApiError::bad_request(code, format!("{field} must be between {min} and {MAX_CREDITS}")));
    }
    Ok(())
}

fn validate_feature(feature: &str) -> Result<(), ApiError> {
    if feature.trim().is_empty() || feature.len() > MAX_ID_LEN || feature.chars().any(char::is_control) {
        return Err(ApiError::bad_request(
            "invalid_feature",
            format!("feature must be 1-{MAX_ID_LEN} characters with no control characters"),
        ));
    }
    Ok(())
}

fn validate_item_key(item_key: &str) -> Result<(), ApiError> {
    if item_key.trim().is_empty() || item_key.len() > MAX_ID_LEN || item_key.chars().any(char::is_control) {
        return Err(ApiError::bad_request(
            "invalid_item_key",
            format!("item_key must be 1-{MAX_ID_LEN} characters with no control characters"),
        ));
    }
    Ok(())
}

impl ReserveRequest {
    pub fn validate(&self) -> Result<(), ApiError> {
        validate_tenant_id(&self.tenant_id)?;
        validate_feature(&self.feature)?;
        validate_credits("estimate_credits", "invalid_estimate_credits", self.estimate_credits, 1)?;
        validate_metadata(self.metadata.as_ref())?;
        let ttl = self.ttl();
        if !(1..=MAX_TTL_SECONDS).contains(&ttl) {
            return Err(ApiError::bad_request(
                "invalid_ttl_seconds",
                format!("ttl_seconds must be between 1 and {MAX_TTL_SECONDS}"),
            ));
        }
        if let Some(item_key) = &self.item_key {
            validate_item_key(item_key)?;
        }
        Ok(())
    }
}

impl CommitRequest {
    pub fn validate(&self) -> Result<(), ApiError> {
        validate_tenant_id(&self.tenant_id)?;
        validate_metadata(self.metadata.as_ref())?;
        validate_credits("actual_credits_used", "invalid_actual_credits_used", self.actual_credits_used, 0)
    }
}

impl ReleaseRequest {
    pub fn validate(&self) -> Result<(), ApiError> {
        validate_tenant_id(&self.tenant_id)?;
        validate_reason(self.reason.as_deref())
    }
}

// ---------------------------------------------------------------------------
// Import (POST /v1/import)
// ---------------------------------------------------------------------------

pub const MAX_IMPORT_TENANTS: usize = 100;
pub const MAX_IMPORT_CREDITS_PER_TENANT: usize = 50;
pub const MAX_IMPORT_PURCHASES: usize = 5_000;

/// One tenant moved over from another billing system. Both parts are
/// optional; unknown fields are refused so a typo (`purchase`) cannot
/// silently drop data.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportTenant {
    pub tenant_id: String,
    /// The starting balance, as one or more grants. Importable once per tenant.
    #[serde(default)]
    pub credits: Vec<ImportCredit>,
    /// Items bought in the old system; `chargeOnce` treats them as paid.
    #[serde(default)]
    pub purchases: Vec<ImportPurchase>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportCredit {
    pub amount: i64,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportPurchase {
    pub item_key: String,
    /// Part of the item's identity: charges under another feature are for a
    /// different item and are not covered by this purchase.
    pub feature: String,
    /// What the tenant paid for it in the old system (at least 1).
    pub credits: i64,
    #[serde(default)]
    pub purchased_at: Option<DateTime<Utc>>,
}

impl ImportTenant {
    pub fn validate(&self) -> Result<(), ApiError> {
        validate_tenant_id(&self.tenant_id)?;
        let invalid = |message: String| Err(ApiError::bad_request("invalid_import", message));
        if self.credits.is_empty() && self.purchases.is_empty() {
            return invalid(format!("tenant '{}' has neither credits nor purchases to import", self.tenant_id));
        }
        if self.credits.len() > MAX_IMPORT_CREDITS_PER_TENANT {
            return invalid(format!("at most {MAX_IMPORT_CREDITS_PER_TENANT} credit grants per tenant"));
        }
        let mut total: i64 = 0;
        for credit in &self.credits {
            validate_reason(credit.reason.as_deref())?;
            if !(1..=MAX_CREDITS).contains(&credit.amount) {
                return invalid(format!("tenant '{}': credit amounts must be between 1 and {MAX_CREDITS}", self.tenant_id));
            }
            total = total
                .checked_add(credit.amount)
                .ok_or_else(|| ApiError::bad_request("invalid_import", "credit amounts overflow"))?;
        }
        let mut seen = std::collections::HashSet::new();
        for purchase in &self.purchases {
            validate_item_key(&purchase.item_key)?;
            validate_feature(&purchase.feature)?;
            if !(1..=MAX_CREDITS).contains(&purchase.credits) {
                return invalid(format!(
                    "tenant '{}': purchase '{}' must have credits of at least 1",
                    self.tenant_id, purchase.item_key
                ));
            }
            if !seen.insert((purchase.feature.as_str(), purchase.item_key.as_str())) {
                return invalid(format!(
                    "tenant '{}': item '{}' under feature '{}' appears twice",
                    self.tenant_id, purchase.item_key, purchase.feature
                ));
            }
        }
        Ok(())
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
