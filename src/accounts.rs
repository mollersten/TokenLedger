//! Account and API key provisioning, shared by the admin CLI
//! (`cargo run --bin admin`), the integration tests and the load generator.
//!
//! API keys look like `tl_live_<64 hex chars>`: 244 random bits from the OS
//! CSPRNG. Only their SHA-256 hash is stored, so a leaked database does not
//! leak usable keys; the plaintext is returned exactly once, at creation.

use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

pub const KEY_PREFIX: &str = "tl_live_";
/// `tl_live_` + two simple UUIDs.
pub const KEY_LEN: usize = KEY_PREFIX.len() + 64;

pub const TRIAL_DAYS: i64 = 14;
pub const TRIAL_MONTHLY_REQUESTS: i64 = 100_000;
pub const TRIAL_MAX_TENANTS: i32 = 1_000;
pub const TRIAL_RATE_LIMIT_PER_SEC: i32 = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plan {
    Trial,
    Paid,
}

impl Plan {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trial => "trial",
            Self::Paid => "paid",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "trial" => Some(Self::Trial),
            "paid" => Some(Self::Paid),
            _ => None,
        }
    }
}

/// Settings for a new account. `None` limits mean unlimited.
#[derive(Debug, Clone)]
pub struct NewAccount {
    pub name: String,
    pub plan: Plan,
    pub trial_ends_at: Option<DateTime<Utc>>,
    pub monthly_request_limit: Option<i64>,
    pub max_tenants: Option<i32>,
    pub rate_limit_per_sec: Option<i32>,
}

impl NewAccount {
    /// A free trial with the default limits.
    pub fn trial(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            plan: Plan::Trial,
            trial_ends_at: Some(Utc::now() + Duration::days(TRIAL_DAYS)),
            monthly_request_limit: Some(TRIAL_MONTHLY_REQUESTS),
            max_tenants: Some(TRIAL_MAX_TENANTS),
            rate_limit_per_sec: Some(TRIAL_RATE_LIMIT_PER_SEC),
        }
    }

    /// A paid account without limits.
    pub fn unlimited(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            plan: Plan::Paid,
            trial_ends_at: None,
            monthly_request_limit: None,
            max_tenants: None,
            rate_limit_per_sec: None,
        }
    }
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct Account {
    pub id: String,
    pub name: String,
    pub plan: String,
    pub status: String,
    pub trial_ends_at: Option<DateTime<Utc>>,
    pub monthly_request_limit: Option<i64>,
    pub max_tenants: Option<i32>,
    pub rate_limit_per_sec: Option<i32>,
    pub created_at: DateTime<Utc>,
}

const ACCOUNT_COLUMNS: &str = "id, name, plan, status, trial_ends_at, monthly_request_limit, \
     max_tenants, rate_limit_per_sec, created_at";

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct AccountSummary {
    #[sqlx(flatten)]
    #[serde(flatten)]
    pub account: Account,
    pub active_keys: i64,
    pub tenants: i64,
    pub requests_this_month: i64,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct KeySummary {
    pub id: String,
    pub key_prefix: String,
    pub label: Option<String>,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// A freshly issued key. `secret` is the only copy of the plaintext.
#[derive(Debug, Clone)]
pub struct IssuedKey {
    pub id: String,
    pub account_id: String,
    pub prefix: String,
    pub secret: String,
}

/// Limit changes: `Some(None)` clears a limit (unlimited), `None` keeps it.
#[derive(Debug, Clone, Default)]
pub struct LimitChange {
    pub monthly_request_limit: Option<Option<i64>>,
    pub max_tenants: Option<Option<i32>>,
    pub rate_limit_per_sec: Option<Option<i32>>,
}

pub fn generate_secret() -> String {
    format!("{KEY_PREFIX}{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

pub fn hash_secret(secret: &str) -> String {
    hex::encode(Sha256::digest(secret.as_bytes()))
}

fn random_id(prefix: &str) -> String {
    format!("{prefix}_{}", &Uuid::new_v4().simple().to_string()[..20])
}

pub async fn create_account(pool: &PgPool, new: &NewAccount) -> Result<Account, sqlx::Error> {
    sqlx::query_as::<_, Account>(&format!(
        "INSERT INTO accounts \
           (id, name, plan, trial_ends_at, monthly_request_limit, max_tenants, rate_limit_per_sec) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING {ACCOUNT_COLUMNS}"
    ))
    .bind(random_id("acct"))
    .bind(&new.name)
    .bind(new.plan.as_str())
    .bind(new.trial_ends_at)
    .bind(new.monthly_request_limit)
    .bind(new.max_tenants)
    .bind(new.rate_limit_per_sec)
    .fetch_one(pool)
    .await
}

pub async fn get_account(pool: &PgPool, account_id: &str) -> Result<Option<Account>, sqlx::Error> {
    sqlx::query_as::<_, Account>(&format!("SELECT {ACCOUNT_COLUMNS} FROM accounts WHERE id = $1"))
        .bind(account_id)
        .fetch_optional(pool)
        .await
}

pub async fn issue_key(
    pool: &PgPool,
    account_id: &str,
    label: Option<&str>,
) -> Result<IssuedKey, sqlx::Error> {
    let secret = generate_secret();
    let prefix = format!("{}…", &secret[..KEY_PREFIX.len() + 8]);
    let id = random_id("key");
    sqlx::query(
        "INSERT INTO api_keys (id, account_id, key_prefix, key_hash, label) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&id)
    .bind(account_id)
    .bind(&prefix)
    .bind(hash_secret(&secret))
    .bind(label)
    .execute(pool)
    .await?;
    Ok(IssuedKey { id, account_id: account_id.to_owned(), prefix, secret })
}

/// Revoke a key. Running servers stop accepting it within their auth cache
/// TTL (30 s by default).
pub async fn revoke_key(pool: &PgPool, key_id: &str) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("UPDATE api_keys SET revoked_at = NOW() WHERE id = $1 AND revoked_at IS NULL")
        .bind(key_id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() == 1)
}

pub async fn set_status(pool: &PgPool, account_id: &str, active: bool) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("UPDATE accounts SET status = $2, updated_at = NOW() WHERE id = $1")
        .bind(account_id)
        .bind(if active { "active" } else { "suspended" })
        .execute(pool)
        .await?;
    Ok(result.rows_affected() == 1)
}

pub async fn set_plan(pool: &PgPool, account_id: &str, plan: Plan) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("UPDATE accounts SET plan = $2, updated_at = NOW() WHERE id = $1")
        .bind(account_id)
        .bind(plan.as_str())
        .execute(pool)
        .await?;
    Ok(result.rows_affected() == 1)
}

/// Set the trial end to `days` from now (or from the current end, if later).
pub async fn extend_trial(
    pool: &PgPool,
    account_id: &str,
    days: i64,
) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    sqlx::query_scalar(
        "UPDATE accounts \
         SET trial_ends_at = GREATEST(COALESCE(trial_ends_at, NOW()), NOW()) + make_interval(days => $2::int), \
             updated_at = NOW() \
         WHERE id = $1 RETURNING trial_ends_at",
    )
    .bind(account_id)
    .bind(days)
    .fetch_optional(pool)
    .await
}

pub async fn set_trial_end(
    pool: &PgPool,
    account_id: &str,
    ends_at: Option<DateTime<Utc>>,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("UPDATE accounts SET trial_ends_at = $2, updated_at = NOW() WHERE id = $1")
        .bind(account_id)
        .bind(ends_at)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() == 1)
}

pub async fn set_limits(pool: &PgPool, account_id: &str, change: &LimitChange) -> Result<bool, sqlx::Error> {
    let mut qb = QueryBuilder::<Postgres>::new("UPDATE accounts SET updated_at = NOW()");
    if let Some(v) = change.monthly_request_limit {
        qb.push(", monthly_request_limit = ").push_bind(v);
    }
    if let Some(v) = change.max_tenants {
        qb.push(", max_tenants = ").push_bind(v);
    }
    if let Some(v) = change.rate_limit_per_sec {
        qb.push(", rate_limit_per_sec = ").push_bind(v);
    }
    qb.push(" WHERE id = ").push_bind(account_id);
    Ok(qb.build().execute(pool).await?.rows_affected() == 1)
}

pub async fn list_accounts(pool: &PgPool) -> Result<Vec<AccountSummary>, sqlx::Error> {
    sqlx::query_as::<_, AccountSummary>(
        "SELECT a.id, a.name, a.plan, a.status, a.trial_ends_at, a.monthly_request_limit, \
                a.max_tenants, a.rate_limit_per_sec, a.created_at, \
                (SELECT COUNT(*) FROM api_keys k WHERE k.account_id = a.id AND k.revoked_at IS NULL) AS active_keys, \
                (SELECT COUNT(*) FROM wallets w WHERE w.account_id = a.id) AS tenants, \
                COALESCE((SELECT u.request_count FROM account_usage u \
                          WHERE u.account_id = a.id \
                            AND u.period_start = date_trunc('month', NOW() AT TIME ZONE 'UTC')::date), 0) \
                  AS requests_this_month \
         FROM accounts a ORDER BY a.created_at",
    )
    .fetch_all(pool)
    .await
}

pub async fn list_keys(pool: &PgPool, account_id: &str) -> Result<Vec<KeySummary>, sqlx::Error> {
    sqlx::query_as::<_, KeySummary>(
        "SELECT id, key_prefix, label, created_at, revoked_at FROM api_keys \
         WHERE account_id = $1 ORDER BY created_at",
    )
    .bind(account_id)
    .fetch_all(pool)
    .await
}
