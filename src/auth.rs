//! API key authentication and plan enforcement for every `/v1` route.
//!
//! Per request, in order (none of these rejections are cached for idempotent
//! replay):
//!
//! 1. `Authorization: Bearer tl_live_…` must be present and well-formed (401).
//! 2. The key's SHA-256 hash must match an unrevoked key (401). Lookups are
//!    cached in memory for [`DEFAULT_AUTH_CACHE_TTL`], so revocations, status,
//!    plan and limit changes take effect within that window.
//! 3. The account must be active (403 `account_suspended`) and, on the trial
//!    plan, inside its trial window (403 `trial_expired`).
//! 4. Per-account token bucket at `rate_limit_per_sec`, shared by all of the
//!    account's keys (429 `rate_limited`, with `Retry-After`).
//! 5. Hourly limit per account and clock hour, UTC (429
//!    `hourly_limit_exceeded`). The first request over `hourly_request_limit`
//!    starts the account's one-time grace period: for
//!    [`GRACE_PERIOD_HOURS`](crate::accounts::GRACE_PERIOD_HOURS) the limit is
//!    `grace_hourly_request_limit` instead. After that the plain limit applies
//!    until the account moves to a plan without one. Accounts with an hourly
//!    limit get `X-RateLimit-*` headers on every response (see
//!    [`HourlyQuota`]).
//! 6. Monthly request quota (429 `monthly_quota_exceeded`).
//!
//! Requests are counted in memory and flushed to `account_hourly_usage` and
//! `account_usage` in batches, so the hot path never writes a per-account
//! row. Only accepted requests are counted.
//!
//! The rate limiter and the unflushed part of each count are per process:
//! with N API instances, an account can burst up to N × its per-second rate,
//! and hourly and monthly checks can overshoot by at most one flush interval
//! of traffic.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    extract::{Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Datelike, NaiveDate, TimeDelta, Utc};
use sqlx::PgPool;
use tokio::task::JoinHandle;

use crate::{
    AppState,
    accounts::{self, KEY_LEN, KEY_PREFIX, hash_secret},
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
    pub hourly_request_limit: Option<i32>,
    pub grace_hourly_request_limit: Option<i32>,
    pub grace_ends_at: Option<DateTime<Utc>>,
    pub max_tenants: Option<i32>,
    pub rate_limit_per_sec: Option<i32>,
    /// Start of the clock hour the hourly figure below belongs to.
    pub usage_hour: DateTime<Utc>,
    /// Requests recorded in `account_hourly_usage` for that hour at lookup time.
    pub hourly_usage_at_lookup: i64,
    /// Start of the calendar month the monthly figure below belongs to.
    pub usage_month: DateTime<Utc>,
    /// Requests recorded in `account_usage` for that month at lookup time.
    pub monthly_usage_at_lookup: i64,
}

/// Start (UTC) of the clock hour containing `now`.
pub fn hour_start(now: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp(now.timestamp().div_euclid(3600) * 3600, 0).expect("valid time")
}

/// Start (UTC) of the calendar month containing `now`.
pub fn month_start(now: DateTime<Utc>) -> DateTime<Utc> {
    NaiveDate::from_ymd_opt(now.year(), now.month(), 1)
        .expect("valid date")
        .and_hms_opt(0, 0, 0)
        .expect("valid time")
        .and_utc()
}

fn next_month(start: DateTime<Utc>) -> DateTime<Utc> {
    month_start(start + TimeDelta::days(32))
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

        let now = Utc::now();
        let ctx = sqlx::query_as::<_, AuthContext>(
            "SELECT a.id AS account_id, k.id AS key_id, a.plan, a.status, a.trial_ends_at, \
                    a.monthly_request_limit, a.hourly_request_limit, a.grace_hourly_request_limit, \
                    a.grace_ends_at, a.max_tenants, a.rate_limit_per_sec, \
                    $2::timestamptz AS usage_hour, COALESCE(h.request_count, 0) AS hourly_usage_at_lookup, \
                    $3::timestamptz AS usage_month, COALESCE(u.request_count, 0) AS monthly_usage_at_lookup \
             FROM api_keys k \
             JOIN accounts a ON a.id = k.account_id \
             LEFT JOIN account_hourly_usage h ON h.account_id = a.id AND h.hour_start = $2 \
             LEFT JOIN account_usage u ON u.account_id = a.id \
                                      AND u.period_start = ($3::timestamptz AT TIME ZONE 'UTC')::date \
             WHERE k.key_hash = $1 AND k.revoked_at IS NULL",
        )
        .bind(&hash)
        .bind(hour_start(now))
        .bind(month_start(now))
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

    /// Record the result of [`accounts::start_grace`] in every cached key of
    /// the account, so this process stops asking the database. Returns the
    /// caller's context with the change applied. `None` means the account
    /// has no grace limit (any more).
    fn record_grace(&self, ctx: &AuthContext, ends: Option<DateTime<Utc>>) -> Arc<AuthContext> {
        let apply = |c: &AuthContext| {
            let mut c = c.clone();
            c.grace_ends_at = ends;
            if ends.is_none() {
                c.grace_hourly_request_limit = None;
            }
            Arc::new(c)
        };
        for (entry, _) in self.entries.lock().unwrap().values_mut() {
            if let Some(cached) = entry
                && cached.account_id == ctx.account_id
            {
                *cached = apply(cached);
            }
        }
        apply(ctx)
    }
}

// ---------------------------------------------------------------------------
// Per-account rate limiter (token bucket, burst = one second of traffic)
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct RateLimiter {
    buckets: Mutex<HashMap<String, (f64, Instant)>>,
}

impl RateLimiter {
    /// Take one token from `account_id`'s bucket, or return how many seconds
    /// until one is available.
    pub fn check(&self, account_id: &str, per_sec: u32) -> Result<(), u64> {
        let rate = f64::from(per_sec.max(1));
        let now = Instant::now();
        let mut buckets = self.buckets.lock().unwrap();
        if !buckets.contains_key(account_id) {
            buckets.insert(account_id.to_owned(), (rate, now));
        }
        let (tokens, updated) = buckets.get_mut(account_id).expect("inserted above");
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
// Hourly and monthly usage meter
// ---------------------------------------------------------------------------

/// Where an account stands against its hourly limit, sent as headers:
///
/// - `X-RateLimit-Limit`: requests allowed this hour (the grace limit while
///   the grace period runs)
/// - `X-RateLimit-Remaining`: requests left this hour
/// - `X-RateLimit-Reset`: when the count resets (Unix seconds)
/// - `X-RateLimit-Grace-Ends`: when the grace period ends (Unix seconds);
///   only sent while it runs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HourlyQuota {
    pub limit: i64,
    pub remaining: i64,
    pub resets_at: DateTime<Utc>,
    pub grace_ends_at: Option<DateTime<Utc>>,
}

pub const HEADER_LIMIT: &str = "x-ratelimit-limit";
pub const HEADER_REMAINING: &str = "x-ratelimit-remaining";
pub const HEADER_RESET: &str = "x-ratelimit-reset";
pub const HEADER_GRACE_ENDS: &str = "x-ratelimit-grace-ends";

impl HourlyQuota {
    fn write_headers(&self, headers: &mut HeaderMap) {
        let mut set = |name: &'static str, value: i64| {
            headers.insert(HeaderName::from_static(name), HeaderValue::from(value));
        };
        set(HEADER_LIMIT, self.limit);
        set(HEADER_REMAINING, self.remaining);
        set(HEADER_RESET, self.resets_at.timestamp());
        if let Some(ends) = self.grace_ends_at {
            set(HEADER_GRACE_ENDS, ends.timestamp());
        }
    }
}

/// Why [`UsageMeter::try_count`] did not count a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The account just went over its hourly limit for the first time:
    /// start its grace period, then ask again.
    StartGrace,
    /// Over the hourly limit in force.
    Hourly(HourlyQuota),
    /// Over the monthly quota (the limit).
    Monthly(i64),
}

/// Requests counted in one window (a clock hour or a calendar month).
#[derive(Debug, Clone, Copy)]
struct Counter {
    start: DateTime<Utc>,
    /// Total in the database as of the last flush (or the auth lookup).
    base: i64,
    /// Counted here but not yet flushed.
    pending: i64,
}

impl Counter {
    fn used(&self) -> i64 {
        self.base + self.pending
    }

    /// Move on to the window starting at `start` if it is later, returning
    /// the unflushed count of the window that ended.
    fn advance(&mut self, start: DateTime<Utc>) -> Option<(DateTime<Utc>, i64)> {
        if start <= self.start {
            return None;
        }
        let ended = (self.pending > 0).then_some((self.start, self.pending));
        *self = Self { start, base: 0, pending: 0 };
        ended
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Window {
    Hour,
    Month,
}

impl Window {
    const ALL: [Self; 2] = [Self::Hour, Self::Month];

    /// Adds counts to the window's table and returns the new totals.
    fn upsert_sql(self) -> &'static str {
        match self {
            Self::Hour => {
                "INSERT INTO account_hourly_usage (account_id, hour_start, request_count) \
                 SELECT * FROM UNNEST($1::text[], $2::timestamptz[], $3::int8[]) \
                 ON CONFLICT (account_id, hour_start) \
                 DO UPDATE SET request_count = account_hourly_usage.request_count + EXCLUDED.request_count \
                 RETURNING account_id, hour_start, request_count"
            }
            Self::Month => {
                "INSERT INTO account_usage (account_id, period_start, request_count) \
                 SELECT a, (s AT TIME ZONE 'UTC')::date, n \
                 FROM UNNEST($1::text[], $2::timestamptz[], $3::int8[]) AS t (a, s, n) \
                 ON CONFLICT (account_id, period_start) \
                 DO UPDATE SET request_count = account_usage.request_count + EXCLUDED.request_count \
                 RETURNING account_id, period_start::timestamp AT TIME ZONE 'UTC', request_count"
            }
        }
    }
}

struct AccountUsage {
    hour: Counter,
    month: Counter,
}

impl AccountUsage {
    fn counter(&mut self, window: Window) -> &mut Counter {
        match window {
            Window::Hour => &mut self.hour,
            Window::Month => &mut self.month,
        }
    }
}

/// (account, window start, count)
type Batch = Vec<(String, DateTime<Utc>, i64)>;

#[derive(Default)]
struct MeterState {
    accounts: HashMap<String, AccountUsage>,
    /// Unflushed counts from hours that have since ended.
    ended_hours: Batch,
    /// Unflushed counts from months that have since ended.
    ended_months: Batch,
}

impl MeterState {
    fn ended(&mut self, window: Window) -> &mut Batch {
        match window {
            Window::Hour => &mut self.ended_hours,
            Window::Month => &mut self.ended_months,
        }
    }
}

#[derive(Default)]
pub struct UsageMeter {
    state: Mutex<MeterState>,
}

impl UsageMeter {
    /// Count one request against the account's hourly and monthly limits.
    /// On success, returns where the account stands for the hour if it has
    /// an hourly limit.
    pub fn try_count(&self, ctx: &AuthContext, now: DateTime<Utc>) -> Result<Option<HourlyQuota>, Refusal> {
        let hour = hour_start(now);
        let month = month_start(now);
        let mut guard = self.state.lock().unwrap();
        let state = &mut *guard;
        if !state.accounts.contains_key(&ctx.account_id) {
            let at_lookup = |start, lookup_start, count| Counter {
                start,
                base: if lookup_start == start { count } else { 0 },
                pending: 0,
            };
            state.accounts.insert(
                ctx.account_id.clone(),
                AccountUsage {
                    hour: at_lookup(hour, ctx.usage_hour, ctx.hourly_usage_at_lookup),
                    month: at_lookup(month, ctx.usage_month, ctx.monthly_usage_at_lookup),
                },
            );
        }
        let usage = state.accounts.get_mut(&ctx.account_id).expect("inserted above");
        if let Some((start, count)) = usage.hour.advance(hour) {
            state.ended_hours.push((ctx.account_id.clone(), start, count));
        }
        if let Some((start, count)) = usage.month.advance(month) {
            state.ended_months.push((ctx.account_id.clone(), start, count));
        }

        let quota = match ctx.hourly_request_limit.map(i64::from) {
            None => None,
            Some(limit) => {
                let grace_limit = ctx.grace_hourly_request_limit.map(|g| i64::from(g).max(limit));
                let grace = match (grace_limit, ctx.grace_ends_at) {
                    (Some(grace_limit), Some(ends)) if now < ends => Some((grace_limit, ends)),
                    _ => None,
                };
                let in_force = grace.map_or(limit, |(grace_limit, _)| grace_limit);
                let used = usage.hour.used();
                let quota = HourlyQuota {
                    limit: in_force,
                    remaining: (in_force - used - 1).max(0),
                    resets_at: hour + TimeDelta::hours(1),
                    grace_ends_at: grace.map(|(_, ends)| ends),
                };
                if used >= in_force {
                    if grace_limit.is_some() && ctx.grace_ends_at.is_none() {
                        return Err(Refusal::StartGrace);
                    }
                    return Err(Refusal::Hourly(HourlyQuota { remaining: 0, ..quota }));
                }
                Some(quota)
            }
        };
        if let Some(limit) = ctx.monthly_request_limit
            && usage.month.used() >= limit
        {
            return Err(Refusal::Monthly(limit));
        }
        usage.hour.pending += 1;
        usage.month.pending += 1;
        Ok(quota)
    }

    /// Write pending counts to `account_hourly_usage` and `account_usage`,
    /// one statement per table. Returns the number of rows updated.
    pub async fn flush(&self, pool: &PgPool) -> Result<usize, sqlx::Error> {
        let mut updated = 0;
        let mut first_error = None;
        for window in Window::ALL {
            match self.flush_window(pool, window).await {
                Ok(rows) => updated += rows,
                Err(err) => {
                    first_error.get_or_insert(err);
                }
            }
        }
        first_error.map_or(Ok(updated), Err)
    }

    async fn flush_window(&self, pool: &PgPool, window: Window) -> Result<usize, sqlx::Error> {
        let mut batch: Batch = {
            let mut guard = self.state.lock().unwrap();
            let state = &mut *guard;
            let mut batch = std::mem::take(state.ended(window));
            for (account, usage) in state.accounts.iter_mut() {
                let counter = usage.counter(window);
                if counter.pending > 0 {
                    batch.push((account.clone(), counter.start, counter.pending));
                    counter.pending = 0;
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

        let result = sqlx::query_as::<_, (String, DateTime<Utc>, i64)>(window.upsert_sql())
            .bind(batch.iter().map(|b| b.0.clone()).collect::<Vec<_>>())
            .bind(batch.iter().map(|b| b.1).collect::<Vec<_>>())
            .bind(batch.iter().map(|b| b.2).collect::<Vec<_>>())
            .fetch_all(pool)
            .await;

        let mut guard = self.state.lock().unwrap();
        let state = &mut *guard;
        match result {
            Ok(rows) => {
                for (account, start, total) in &rows {
                    if let Some(usage) = state.accounts.get_mut(account) {
                        let counter = usage.counter(window);
                        if counter.start == *start {
                            counter.base = *total;
                        }
                    }
                }
                Ok(rows.len())
            }
            Err(err) => {
                // Put the counts back so the next flush retries them.
                for (account, start, delta) in batch {
                    match state.accounts.get_mut(&account).map(|u| u.counter(window)) {
                        Some(counter) if counter.start == start => counter.pending += delta,
                        _ => state.ended(window).push((account, start, delta)),
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

pub struct Admitted {
    pub ctx: Arc<AuthContext>,
    pub hourly: Option<HourlyQuota>,
}

pub struct Rejected {
    pub error: ApiError,
    /// Sent as headers on hourly-limit rejections.
    pub hourly: Option<HourlyQuota>,
}

impl From<ApiError> for Rejected {
    fn from(error: ApiError) -> Self {
        Self { error, hourly: None }
    }
}

impl From<sqlx::Error> for Rejected {
    fn from(error: sqlx::Error) -> Self {
        ApiError::from(error).into()
    }
}

fn seconds_until(when: DateTime<Utc>, now: DateTime<Utc>) -> u64 {
    (when - now).num_seconds().max(1) as u64
}

fn hourly_limit_error(ctx: &AuthContext, quota: HourlyQuota, now: DateTime<Utc>) -> ApiError {
    let plan = &ctx.plan;
    let limit = quota.limit;
    let message = match (quota.grace_ends_at, ctx.grace_ends_at) {
        (Some(ends), _) => format!(
            "this account has used all {limit} requests for this hour allowed during its grace period, \
             which ends at {}; upgrade the plan to remove the limit",
            ends.to_rfc3339()
        ),
        (None, Some(ended)) if ctx.grace_hourly_request_limit.is_some() => format!(
            "the {plan} plan allows {limit} requests per hour and this account's grace period ended at {}; \
             upgrade the plan to send more",
            ended.to_rfc3339()
        ),
        _ => format!("the {plan} plan allows {limit} requests per hour"),
    };
    ApiError::too_many_requests("hourly_limit_exceeded", message, Some(seconds_until(quota.resets_at, now)))
}

pub async fn authorize(state: &AppState, headers: &HeaderMap) -> Result<Admitted, Rejected> {
    let token = bearer_token(headers).ok_or_else(|| {
        ApiError::unauthorized(
            "missing_api_key",
            "send your API key in the header 'Authorization: Bearer tl_live_…'",
        )
    })?;
    let invalid = || ApiError::unauthorized("invalid_api_key", "the API key is invalid or has been revoked");
    if token.len() != KEY_LEN || !token.starts_with(KEY_PREFIX) {
        return Err(invalid().into());
    }
    let mut ctx = state.auth.resolve(&state.pool, token).await?.ok_or_else(invalid)?;

    if ctx.status != "active" {
        return Err(ApiError::forbidden("account_suspended", "this account is suspended").into());
    }
    let now = Utc::now();
    if ctx.plan == "trial"
        && let Some(ends) = ctx.trial_ends_at
        && ends <= now
    {
        return Err(ApiError::forbidden(
            "trial_expired",
            format!("the free trial ended at {}", ends.to_rfc3339()),
        )
        .into());
    }
    if let Some(rate) = ctx.rate_limit_per_sec
        && let Err(retry_after) = state.limiter.check(&ctx.account_id, rate.max(1) as u32)
    {
        return Err(ApiError::too_many_requests(
            "rate_limited",
            format!("this account is limited to {rate} requests per second"),
            Some(retry_after),
        )
        .into());
    }

    let mut outcome = state.usage.try_count(&ctx, now);
    if outcome == Err(Refusal::StartGrace) {
        let ends = accounts::start_grace(&state.pool, &ctx.account_id).await?;
        if let Some(ends) = ends
            && now < ends
        {
            tracing::info!(
                account = %ctx.account_id,
                plan = %ctx.plan,
                grace_ends_at = %ends.to_rfc3339(),
                "account went over its hourly limit; grace period running"
            );
        }
        ctx = state.auth.record_grace(&ctx, ends);
        outcome = state.usage.try_count(&ctx, now);
    }

    match outcome {
        Ok(hourly) => Ok(Admitted { ctx, hourly }),
        Err(Refusal::Hourly(quota)) => {
            Err(Rejected { error: hourly_limit_error(&ctx, quota, now), hourly: Some(quota) })
        }
        Err(Refusal::Monthly(limit)) => Err(ApiError::too_many_requests(
            "monthly_quota_exceeded",
            format!("this account has used all {limit} requests for this month"),
            Some(seconds_until(next_month(month_start(now)), now)),
        )
        .into()),
        // `record_grace` sets either an end time or no grace limit, so a
        // second request to start the grace period cannot happen.
        Err(Refusal::StartGrace) => Err(ApiError::internal("grace period did not start").into()),
    }
}

pub async fn require_api_key(State(state): State<AppState>, mut request: Request, next: Next) -> Response {
    match authorize(&state, request.headers()).await {
        Ok(Admitted { ctx, hourly }) => {
            request.extensions_mut().insert(ctx);
            let mut response = next.run(request).await;
            if let Some(quota) = hourly {
                quota.write_headers(response.headers_mut());
            }
            response
        }
        Err(Rejected { error, hourly }) => {
            let mut response = error.into_response();
            if let Some(quota) = hourly {
                quota.write_headers(response.headers_mut());
            }
            response
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn at(h: u32, m: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 16, h, m, 0).unwrap()
    }

    /// A free-plan account first seen at `now` with nothing recorded yet.
    fn free_ctx(now: DateTime<Utc>) -> AuthContext {
        AuthContext {
            account_id: "acct_test".into(),
            key_id: "key_test".into(),
            plan: "free".into(),
            status: "active".into(),
            trial_ends_at: None,
            monthly_request_limit: None,
            hourly_request_limit: Some(3),
            grace_hourly_request_limit: Some(5),
            grace_ends_at: None,
            max_tenants: None,
            rate_limit_per_sec: None,
            usage_hour: hour_start(now),
            hourly_usage_at_lookup: 0,
            usage_month: month_start(now),
            monthly_usage_at_lookup: 0,
        }
    }

    fn admit(meter: &UsageMeter, ctx: &AuthContext, now: DateTime<Utc>, n: usize) -> Vec<Result<i64, Refusal>> {
        (0..n).map(|_| meter.try_count(ctx, now).map(|q| q.unwrap().remaining)).collect()
    }

    #[test]
    fn windows() {
        let t = Utc.with_ymd_and_hms(2026, 12, 31, 23, 59, 59).unwrap();
        assert_eq!(hour_start(t), Utc.with_ymd_and_hms(2026, 12, 31, 23, 0, 0).unwrap());
        assert_eq!(month_start(t), Utc.with_ymd_and_hms(2026, 12, 1, 0, 0, 0).unwrap());
        assert_eq!(next_month(month_start(t)), Utc.with_ymd_and_hms(2027, 1, 1, 0, 0, 0).unwrap());
        let feb = Utc.with_ymd_and_hms(2027, 1, 31, 0, 0, 0).unwrap();
        assert_eq!(next_month(month_start(feb)), Utc.with_ymd_and_hms(2027, 2, 1, 0, 0, 0).unwrap());
    }

    #[test]
    fn hourly_limit_grace_period_and_after() {
        let meter = UsageMeter::default();
        let mut ctx = free_ctx(at(10, 0));

        assert_eq!(admit(&meter, &ctx, at(10, 5), 3), vec![Ok(2), Ok(1), Ok(0)]);
        // The first request over the limit asks for the grace period.
        assert_eq!(meter.try_count(&ctx, at(10, 6)), Err(Refusal::StartGrace));

        ctx.grace_ends_at = Some(at(10, 6) + TimeDelta::hours(24));
        let quota = meter.try_count(&ctx, at(10, 6)).unwrap().unwrap();
        assert_eq!((quota.limit, quota.remaining), (5, 1));
        assert_eq!(quota.grace_ends_at, ctx.grace_ends_at);
        assert_eq!(quota.resets_at, at(11, 0));
        assert_eq!(meter.try_count(&ctx, at(10, 7)).unwrap().unwrap().remaining, 0);
        let Err(Refusal::Hourly(refused)) = meter.try_count(&ctx, at(10, 8)) else { panic!() };
        assert_eq!((refused.limit, refused.remaining), (5, 0));

        // A new hour starts from zero, still at the grace limit.
        assert_eq!(admit(&meter, &ctx, at(11, 0), 5), vec![Ok(4), Ok(3), Ok(2), Ok(1), Ok(0)]);

        // After the grace period the plain limit applies and is never
        // extended again.
        ctx.grace_ends_at = Some(at(11, 30));
        assert_eq!(
            meter.try_count(&ctx, at(11, 31)),
            Err(Refusal::Hourly(HourlyQuota { limit: 3, remaining: 0, resets_at: at(12, 0), grace_ends_at: None }))
        );
        assert_eq!(admit(&meter, &ctx, at(12, 0), 3), vec![Ok(2), Ok(1), Ok(0)]);
        assert!(matches!(meter.try_count(&ctx, at(12, 1)), Err(Refusal::Hourly(_))));
    }

    #[test]
    fn without_a_grace_limit_the_hourly_limit_is_hard() {
        let meter = UsageMeter::default();
        let mut ctx = free_ctx(at(10, 0));
        ctx.grace_hourly_request_limit = None;
        assert_eq!(admit(&meter, &ctx, at(10, 0), 3).len(), 3);
        assert!(matches!(meter.try_count(&ctx, at(10, 0)), Err(Refusal::Hourly(_))));
    }

    #[test]
    fn counts_recorded_elsewhere_are_included() {
        let meter = UsageMeter::default();
        let mut ctx = free_ctx(at(10, 0));
        ctx.hourly_usage_at_lookup = 2;
        assert_eq!(meter.try_count(&ctx, at(10, 1)).unwrap().unwrap().remaining, 0);
        assert_eq!(meter.try_count(&ctx, at(10, 1)), Err(Refusal::StartGrace));

        // A lookup from an earlier hour does not count against this one.
        let meter = UsageMeter::default();
        ctx.hourly_usage_at_lookup = 50;
        assert_eq!(meter.try_count(&ctx, at(11, 0)).unwrap().unwrap().remaining, 2);
    }

    #[test]
    fn accounts_without_an_hourly_limit_are_still_counted() {
        let meter = UsageMeter::default();
        let mut ctx = free_ctx(at(10, 0));
        ctx.hourly_request_limit = None;
        ctx.monthly_request_limit = Some(4);
        assert_eq!(meter.try_count(&ctx, at(10, 0)), Ok(None));
        let state = meter.state.lock().unwrap();
        let usage = &state.accounts["acct_test"];
        assert_eq!((usage.hour.pending, usage.month.pending), (1, 1));
    }

    #[test]
    fn ended_windows_are_kept_for_the_next_flush() {
        let meter = UsageMeter::default();
        let mut ctx = free_ctx(at(10, 0));
        ctx.hourly_request_limit = None;
        admit_unlimited(&meter, &ctx, at(10, 59), 2);
        admit_unlimited(&meter, &ctx, at(11, 0), 1);
        // A request that read the clock just before the hour turned counts
        // in the new hour rather than reopening the old one.
        admit_unlimited(&meter, &ctx, at(10, 59), 1);
        let state = meter.state.lock().unwrap();
        assert_eq!(state.ended_hours, vec![("acct_test".to_owned(), at(10, 0), 2)]);
        assert_eq!(state.accounts["acct_test"].hour.pending, 2);
        assert!(state.ended_months.is_empty());
        assert_eq!(state.accounts["acct_test"].month.pending, 4);
    }

    fn admit_unlimited(meter: &UsageMeter, ctx: &AuthContext, now: DateTime<Utc>, n: usize) {
        for _ in 0..n {
            assert_eq!(meter.try_count(ctx, now), Ok(None));
        }
    }

    #[test]
    fn token_bucket_allows_burst_then_limits() {
        let limiter = RateLimiter::default();
        let allowed = (0..10).filter(|_| limiter.check("acct", 5).is_ok()).count();
        assert_eq!(allowed, 5);
        assert_eq!(limiter.check("acct", 5), Err(1));
        assert!(limiter.check("other", 5).is_ok(), "buckets are per account");
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
