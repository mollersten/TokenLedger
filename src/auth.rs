//! API key authentication and plan enforcement for every `/v1` route.
//!
//! Per request, in order (none of these rejections are cached for idempotent
//! replay):
//!
//! 1. `Authorization: Bearer tl_live_…` must be present and well-formed (401).
//! 2. The key's SHA-256 hash must match an unrevoked key (401). Lookups are
//!    cached in memory for [`DEFAULT_AUTH_CACHE_TTL`], so revocations, status
//!    and limit changes take effect within that window.
//! 3. The account must be active (403 `account_suspended`) and, on the trial
//!    plan, inside its trial window (403 `trial_expired`).
//! 4. Per-key token bucket at `rate_limit_per_sec` (429 `rate_limited`, with
//!    `Retry-After`).
//! 5. Monthly request quota (429 `monthly_quota_exceeded`). Requests are
//!    counted in memory and flushed to `account_usage` in batches, so the
//!    hot path never writes a per-account row.
//!
//! The rate limiter and the in-memory part of the quota are per process: with
//! N API instances, a key can burst up to N × its rate, and quota checks can
//! overshoot by at most one flush interval of traffic.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    extract::{Request, State},
    http::{HeaderMap, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use sqlx::PgPool;
use tokio::task::JoinHandle;

use crate::{
    AppState,
    accounts::{KEY_LEN, KEY_PREFIX, hash_secret},
    error::ApiError,
};

pub const DEFAULT_AUTH_CACHE_TTL: Duration = Duration::from_secs(30);
pub const DEFAULT_USAGE_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const MAX_CACHED_KEYS: usize = 10_000;

/// Who is calling, attached to the request by [`require_api_key`].
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AuthContext {
    pub account_id: String,
    pub key_id: String,
    pub plan: String,
    pub status: String,
    pub trial_ends_at: Option<DateTime<Utc>>,
    pub monthly_request_limit: Option<i64>,
    pub max_tenants: Option<i32>,
    pub rate_limit_per_sec: Option<i32>,
    /// Calendar month the usage figure below belongs to.
    pub usage_period: NaiveDate,
    /// Requests recorded in `account_usage` for that month at lookup time.
    pub usage_at_lookup: i64,
}

/// First day (UTC) of the month containing `now`.
pub fn current_period(now: DateTime<Utc>) -> NaiveDate {
    NaiveDate::from_ymd_opt(now.year(), now.month(), 1).expect("valid date")
}

fn next_period(period: NaiveDate) -> NaiveDate {
    let (y, m) = if period.month() == 12 { (period.year() + 1, 1) } else { (period.year(), period.month() + 1) };
    NaiveDate::from_ymd_opt(y, m, 1).expect("valid date")
}

// ---------------------------------------------------------------------------
// Key lookup cache
// ---------------------------------------------------------------------------

/// Key hash -> (lookup result, when it was fetched). `None` caches a miss.
type CacheEntries = HashMap<String, (Option<Arc<AuthContext>>, Instant)>;

pub struct AuthCache {
    ttl: Duration,
    entries: Mutex<CacheEntries>,
}

impl AuthCache {
    pub fn new(ttl: Duration) -> Self {
        Self { ttl, entries: Mutex::new(HashMap::new()) }
    }

    pub async fn resolve(&self, pool: &PgPool, secret: &str) -> Result<Option<Arc<AuthContext>>, sqlx::Error> {
        let hash = hash_secret(secret);
        if !self.ttl.is_zero()
            && let Some((ctx, fetched)) = self.entries.lock().unwrap().get(&hash)
            && fetched.elapsed() < self.ttl
        {
            return Ok(ctx.clone());
        }

        let period = current_period(Utc::now());
        let ctx = sqlx::query_as::<_, AuthContext>(
            "SELECT a.id AS account_id, k.id AS key_id, a.plan, a.status, a.trial_ends_at, \
                    a.monthly_request_limit, a.max_tenants, a.rate_limit_per_sec, \
                    $2::date AS usage_period, COALESCE(u.request_count, 0) AS usage_at_lookup \
             FROM api_keys k \
             JOIN accounts a ON a.id = k.account_id \
             LEFT JOIN account_usage u ON u.account_id = a.id AND u.period_start = $2 \
             WHERE k.key_hash = $1 AND k.revoked_at IS NULL",
        )
        .bind(&hash)
        .bind(period)
        .fetch_optional(pool)
        .await?
        .map(Arc::new);

        if !self.ttl.is_zero() {
            let mut entries = self.entries.lock().unwrap();
            if entries.len() >= MAX_CACHED_KEYS {
                entries.clear();
            }
            entries.insert(hash, (ctx.clone(), Instant::now()));
        }
        Ok(ctx)
    }
}

// ---------------------------------------------------------------------------
// Per-key rate limiter (token bucket, burst = one second of traffic)
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct RateLimiter {
    buckets: Mutex<HashMap<String, (f64, Instant)>>,
}

impl RateLimiter {
    /// Take one token, or return how many seconds until one is available.
    pub fn check(&self, key_id: &str, per_sec: u32) -> Result<(), u64> {
        let rate = f64::from(per_sec.max(1));
        let now = Instant::now();
        let mut buckets = self.buckets.lock().unwrap();
        let (tokens, updated) = buckets.entry(key_id.to_owned()).or_insert((rate, now));
        *tokens = (*tokens + now.duration_since(*updated).as_secs_f64() * rate).min(rate);
        *updated = now;
        if *tokens >= 1.0 {
            *tokens -= 1.0;
            Ok(())
        } else {
            Err(((1.0 - *tokens) / rate).ceil().max(1.0) as u64)
        }
    }
}

// ---------------------------------------------------------------------------
// Monthly usage meter
// ---------------------------------------------------------------------------

struct Usage {
    period: NaiveDate,
    /// Total in `account_usage` as of the last flush (or the auth lookup).
    base: i64,
    /// Counted here but not yet flushed.
    pending: i64,
}

#[derive(Default)]
struct MeterState {
    accounts: HashMap<String, Usage>,
    /// Unflushed counts from a month that has since ended.
    carried: Vec<(String, NaiveDate, i64)>,
}

#[derive(Default)]
pub struct UsageMeter {
    state: Mutex<MeterState>,
}

impl UsageMeter {
    /// Count one request. Returns the limit if the account has used it up.
    pub fn try_count(&self, ctx: &AuthContext, now: DateTime<Utc>) -> Result<(), i64> {
        let period = current_period(now);
        let mut guard = self.state.lock().unwrap();
        let state = &mut *guard;
        let usage = state.accounts.entry(ctx.account_id.clone()).or_insert_with(|| Usage {
            period,
            base: if ctx.usage_period == period { ctx.usage_at_lookup } else { 0 },
            pending: 0,
        });
        if usage.period != period {
            if usage.pending > 0 {
                state.carried.push((ctx.account_id.clone(), usage.period, usage.pending));
            }
            *usage = Usage { period, base: 0, pending: 0 };
        }
        if let Some(limit) = ctx.monthly_request_limit
            && usage.base + usage.pending >= limit
        {
            return Err(limit);
        }
        usage.pending += 1;
        Ok(())
    }

    /// Write pending counts to `account_usage` in one statement. Returns the
    /// number of account-months updated.
    pub async fn flush(&self, pool: &PgPool) -> Result<usize, sqlx::Error> {
        let mut batch: Vec<(String, NaiveDate, i64)> = {
            let mut state = self.state.lock().unwrap();
            let mut batch = std::mem::take(&mut state.carried);
            for (account, usage) in state.accounts.iter_mut() {
                if usage.pending > 0 {
                    batch.push((account.clone(), usage.period, usage.pending));
                    usage.pending = 0;
                }
            }
            batch
        };
        if batch.is_empty() {
            return Ok(0);
        }
        // A consistent row order keeps concurrent flushes from several
        // instances from deadlocking.
        batch.sort();

        let result = sqlx::query_as::<_, (String, NaiveDate, i64)>(
            "INSERT INTO account_usage (account_id, period_start, request_count) \
             SELECT * FROM UNNEST($1::text[], $2::date[], $3::int8[]) \
             ON CONFLICT (account_id, period_start) \
             DO UPDATE SET request_count = account_usage.request_count + EXCLUDED.request_count \
             RETURNING account_id, period_start, request_count",
        )
        .bind(batch.iter().map(|b| b.0.clone()).collect::<Vec<_>>())
        .bind(batch.iter().map(|b| b.1).collect::<Vec<_>>())
        .bind(batch.iter().map(|b| b.2).collect::<Vec<_>>())
        .fetch_all(pool)
        .await;

        let mut guard = self.state.lock().unwrap();
        let state = &mut *guard;
        match result {
            Ok(rows) => {
                for (account, period, total) in &rows {
                    if let Some(usage) = state.accounts.get_mut(account)
                        && usage.period == *period
                    {
                        usage.base = *total;
                    }
                }
                Ok(rows.len())
            }
            Err(err) => {
                // Put the counts back so the next flush retries them.
                for (account, period, delta) in batch {
                    match state.accounts.get_mut(&account) {
                        Some(usage) if usage.period == period => usage.pending += delta,
                        _ => state.carried.push((account, period, delta)),
                    }
                }
                Err(err)
            }
        }
    }
}

pub fn spawn_usage_flusher(pool: PgPool, meter: Arc<UsageMeter>, interval: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(err) = meter.flush(&pool).await {
                tracing::warn!(error = %err, "usage flush failed; will retry");
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?.trim();
    let (scheme, token) = value.split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then(|| token.trim())
}

pub async fn authorize(state: &AppState, headers: &HeaderMap) -> Result<Arc<AuthContext>, ApiError> {
    let token = bearer_token(headers).ok_or_else(|| {
        ApiError::unauthorized(
            "missing_api_key",
            "send your API key in the header 'Authorization: Bearer tl_live_…'",
        )
    })?;
    let invalid = || ApiError::unauthorized("invalid_api_key", "the API key is invalid or has been revoked");
    if token.len() != KEY_LEN || !token.starts_with(KEY_PREFIX) {
        return Err(invalid());
    }
    let ctx = state.auth.resolve(&state.pool, token).await?.ok_or_else(invalid)?;

    if ctx.status != "active" {
        return Err(ApiError::forbidden("account_suspended", "this account is suspended"));
    }
    let now = Utc::now();
    if ctx.plan == "trial"
        && let Some(ends) = ctx.trial_ends_at
        && ends <= now
    {
        return Err(ApiError::forbidden(
            "trial_expired",
            format!("the free trial ended at {}", ends.to_rfc3339()),
        ));
    }
    if let Some(rate) = ctx.rate_limit_per_sec
        && let Err(retry_after) = state.limiter.check(&ctx.key_id, rate.max(1) as u32)
    {
        return Err(ApiError::too_many_requests(
            "rate_limited",
            format!("this API key is limited to {rate} requests per second"),
            Some(retry_after),
        ));
    }
    if let Err(limit) = state.usage.try_count(&ctx, now) {
        let reset = next_period(current_period(now))
            .and_hms_opt(0, 0, 0)
            .expect("valid time")
            .and_utc();
        return Err(ApiError::too_many_requests(
            "monthly_quota_exceeded",
            format!("this account has used all {limit} requests for this month"),
            Some((reset - now).num_seconds().max(1) as u64),
        ));
    }
    Ok(ctx)
}

pub async fn require_api_key(State(state): State<AppState>, mut request: Request, next: Next) -> Response {
    match authorize(&state, request.headers()).await {
        Ok(ctx) => {
            request.extensions_mut().insert(ctx);
            next.run(request).await
        }
        Err(err) => err.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn period_rolls_over_year() {
        let dec = NaiveDate::from_ymd_opt(2026, 12, 1).unwrap();
        assert_eq!(next_period(dec), NaiveDate::from_ymd_opt(2027, 1, 1).unwrap());
    }

    #[test]
    fn token_bucket_allows_burst_then_limits() {
        let limiter = RateLimiter::default();
        let allowed = (0..10).filter(|_| limiter.check("key", 5).is_ok()).count();
        assert_eq!(allowed, 5);
        assert_eq!(limiter.check("key", 5), Err(1));
        assert!(limiter.check("other", 5).is_ok(), "buckets are per key");
    }

    #[test]
    fn bearer_parsing() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "bearer  abc ".parse().unwrap());
        assert_eq!(bearer_token(&headers), Some("abc"));
        headers.insert(header::AUTHORIZATION, "Basic abc".parse().unwrap());
        assert_eq!(bearer_token(&headers), None);
    }
}
