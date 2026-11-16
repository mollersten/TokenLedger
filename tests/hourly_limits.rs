//! The free plan: requests per hour per account, the one-time grace period,
//! the per-second limit, and upgrading out of it.

mod common;

use std::{sync::Arc, time::Duration};

use axum::http::{Method, StatusCode, header};
use backend::{
    AppState,
    accounts::{self, NewAccount, Plan},
    auth::{self, HEADER_GRACE_ENDS, HEADER_LIMIT, HEADER_REMAINING, HEADER_RESET},
    router,
};
use chrono::{DateTime, TimeDelta, Timelike, Utc};
use common::{Api, TestApp, TestResponse, send, unique_tenant};
use tokio::sync::Barrier;

fn header_num(res: &TestResponse, name: &str) -> Option<i64> {
    res.headers.get(name).map(|v| v.to_str().unwrap().parse().unwrap())
}

/// `(limit, remaining, grace ends)` from the rate-limit headers.
fn quota(res: &TestResponse) -> (i64, i64, Option<i64>) {
    (
        header_num(res, HEADER_LIMIT).unwrap_or_else(|| panic!("no rate-limit headers: {:?}", res.headers)),
        header_num(res, HEADER_REMAINING).unwrap(),
        header_num(res, HEADER_GRACE_ENDS),
    )
}

/// These tests count requests within one clock hour, so don't start one in
/// the last minute of an hour.
async fn away_from_hour_boundary() {
    let now = Utc::now();
    if now.minute() == 59 {
        let wait = 61 - u64::from(now.second());
        tokio::time::sleep(Duration::from_secs(wait)).await;
    }
}

/// A free account with the per-second limit lifted, so tests can send
/// hundreds of requests quickly. A zero auth cache TTL makes plan changes
/// visible immediately.
async fn free_app_without_per_second_limit() -> TestApp {
    let mut account = NewAccount::free("free plan test");
    account.rate_limit_per_sec = None;
    TestApp::spawn_full(account, Duration::ZERO).await
}

async fn grace_ends_at(app: &TestApp) -> Option<DateTime<Utc>> {
    accounts::get_account(&app.pool, &app.account_id).await.unwrap().unwrap().grace_ends_at
}

async fn hourly_count(app: &TestApp) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(request_count), 0)::int8 FROM account_hourly_usage \
         WHERE account_id = $1 AND hour_start = $2",
    )
    .bind(&app.account_id)
    .bind(auth::hour_start(Utc::now()))
    .fetch_one(&app.pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn free_plan_has_the_documented_limits() {
    away_from_hour_boundary().await;
    let free = NewAccount::free("free");
    assert_eq!(free.plan, Plan::Free);
    assert_eq!(free.hourly_request_limit, Some(120));
    assert_eq!(free.grace_hourly_request_limit, Some(600));
    assert_eq!(free.rate_limit_per_sec, Some(10));
    assert_eq!((free.trial_ends_at, free.monthly_request_limit, free.max_tenants), (None, None, None));

    let app = TestApp::spawn_with(free).await;
    let tenant = unique_tenant("cus_free");
    let res = app.grant(&tenant, 10).await;
    assert_eq!(quota(&res), (120, 119, None));
    let reset = auth::hour_start(Utc::now()) + TimeDelta::hours(1);
    assert_eq!(header_num(&res, HEADER_RESET), Some(reset.timestamp()));

    let res = app.get(&format!("/v1/tenants/{tenant}/wallet")).await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(quota(&res), (120, 118, None));

    // Accounts without an hourly limit get no rate-limit headers.
    let paid = TestApp::spawn().await;
    let res = paid.grant(&tenant, 10).await;
    assert!(res.headers.get(HEADER_LIMIT).is_none());
}

#[tokio::test]
async fn free_plan_allows_ten_requests_per_second_across_all_keys() {
    away_from_hour_boundary().await;
    let app = TestApp::spawn_with(NewAccount::free("per second")).await;
    let second = accounts::issue_key(&app.pool, &app.account_id, None).await.unwrap();
    let keys = [app.api.clone(), app.api.with_key(Some(&second.secret))];
    let path = format!("/v1/tenants/{}/wallet", unique_tenant("cus_burst"));

    let mut allowed = 0;
    let mut limited = Vec::new();
    for i in 0..14 {
        let res = send(&keys[i % 2], Method::GET, &path, None, None).await;
        match res.status {
            StatusCode::NOT_FOUND => allowed += 1,
            StatusCode::TOO_MANY_REQUESTS => limited.push(res),
            other => panic!("unexpected {other}: {}", res.body),
        }
    }
    assert!((10..=11).contains(&allowed), "both keys share one 10/s budget, got {allowed}");
    assert_eq!(limited[0].body["error"], "rate_limited");
    assert_eq!(limited[0].headers.get(header::RETRY_AFTER).unwrap(), "1");
}

#[tokio::test]
async fn going_over_the_hourly_limit_starts_a_one_time_grace_period() {
    away_from_hour_boundary().await;
    let app = free_app_without_per_second_limit().await;
    let tenant = unique_tenant("cus_grace");
    app.grant(&tenant, 1_000).await;
    let path = format!("/v1/tenants/{tenant}/wallet");

    // Requests 2..=120 are inside the free limit.
    for _ in 2..=120 {
        assert_eq!(app.get(&path).await.status, StatusCode::OK);
    }
    assert_eq!(grace_ends_at(&app).await, None, "no grace period before the limit is passed");

    // Request 121 goes over: the grace period starts and the request is
    // allowed at the grace limit.
    let before = Utc::now();
    let res = app.get(&path).await;
    let after = Utc::now();
    assert_eq!(res.status, StatusCode::OK, "{}", res.body);
    let ends = grace_ends_at(&app).await.expect("grace period started");
    assert!(ends >= before + TimeDelta::hours(24) - TimeDelta::seconds(1));
    assert!(ends <= after + TimeDelta::hours(24) + TimeDelta::seconds(1));
    assert_eq!(quota(&res), (600, 479, Some(ends.timestamp())));

    for _ in 122..=600 {
        assert_eq!(app.get(&path).await.status, StatusCode::OK);
    }
    let res = app.get(&path).await;
    assert_eq!(res.status, StatusCode::TOO_MANY_REQUESTS, "601st request in the hour");
    assert_eq!(res.body["error"], "hourly_limit_exceeded");
    assert!(res.body["message"].as_str().unwrap().contains("grace period"), "{}", res.body);
    assert_eq!(quota(&res), (600, 0, Some(ends.timestamp())));
    let retry: i64 = res.headers.get(header::RETRY_AFTER).unwrap().to_str().unwrap().parse().unwrap();
    assert!((1..=3600).contains(&retry), "retry when the hour resets: {retry}");

    // Once the grace period is over the free limit applies again, and going
    // over it does not start another one.
    let ended = Utc::now() - TimeDelta::minutes(1);
    accounts::set_grace_end(&app.pool, &app.account_id, Some(ended)).await.unwrap();
    let res = app.get(&path).await;
    assert_eq!(res.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(res.body["error"], "hourly_limit_exceeded");
    assert!(res.body["message"].as_str().unwrap().contains("grace period ended"), "{}", res.body);
    assert_eq!(quota(&res), (120, 0, None));
    assert_eq!(grace_ends_at(&app).await.unwrap().timestamp_micros(), ended.timestamp_micros());

    // Upgrading lifts the limits straight away.
    accounts::set_plan(&app.pool, &app.account_id, Plan::Paid, false).await.unwrap();
    let res = app.get(&path).await;
    assert_eq!(res.status, StatusCode::OK);
    assert!(res.headers.get(HEADER_LIMIT).is_none());
    let paid = accounts::get_account(&app.pool, &app.account_id).await.unwrap().unwrap();
    assert_eq!(
        (paid.hourly_request_limit, paid.grace_hourly_request_limit, paid.rate_limit_per_sec),
        (None, None, None)
    );

    // Moving back to free brings the limits back, without a new grace period.
    accounts::set_plan(&app.pool, &app.account_id, Plan::Free, false).await.unwrap();
    let res = app.get(&path).await;
    assert_eq!(res.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(quota(&res), (120, 0, None));
    assert_eq!(grace_ends_at(&app).await.unwrap().timestamp_micros(), ended.timestamp_micros());

    // Only accepted requests are counted: 600 within the limits plus one
    // while paid.
    app.state.usage.flush(&app.pool).await.unwrap();
    assert_eq!(hourly_count(&app).await, 601);
}

#[tokio::test]
async fn grace_period_can_be_extended_or_reset() {
    away_from_hour_boundary().await;
    let mut account = NewAccount::free("grace admin");
    account.rate_limit_per_sec = None;
    account.hourly_request_limit = Some(2);
    account.grace_hourly_request_limit = Some(4);
    let app = TestApp::spawn_full(account, Duration::ZERO).await;
    let path = format!("/v1/tenants/{}/wallet", unique_tenant("cus_admin"));

    // Extending a grace period that has not started starts it now.
    let ends = accounts::extend_grace(&app.pool, &app.account_id, 2).await.unwrap().unwrap();
    assert!((ends - Utc::now() - TimeDelta::hours(2)).num_seconds().abs() <= 2);
    let res = app.get(&path).await;
    assert_eq!(quota(&res), (4, 3, Some(ends.timestamp())));

    accounts::set_grace_end(&app.pool, &app.account_id, Some(Utc::now() - TimeDelta::seconds(1))).await.unwrap();
    assert_eq!(quota(&app.get(&path).await), (2, 0, None));
    assert_eq!(app.get(&path).await.status, StatusCode::TOO_MANY_REQUESTS);

    // After a reset the next request over the limit starts a fresh 24 hours.
    accounts::set_grace_end(&app.pool, &app.account_id, None).await.unwrap();
    let res = app.get(&path).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND, "admitted (the wallet just doesn't exist)");
    let ends = grace_ends_at(&app).await.unwrap();
    assert!((ends - Utc::now() - TimeDelta::hours(24)).num_seconds().abs() <= 2);
    assert_eq!(quota(&res), (4, 1, Some(ends.timestamp())));

    // Without a grace limit the hourly limit is hard.
    let change = accounts::LimitChange { grace_hourly_request_limit: Some(None), ..Default::default() };
    accounts::set_limits(&app.pool, &app.account_id, &change).await.unwrap();
    accounts::set_grace_end(&app.pool, &app.account_id, None).await.unwrap();
    let res = app.get(&path).await;
    assert_eq!(res.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(quota(&res), (2, 0, None));
    assert_eq!(grace_ends_at(&app).await, None);
}

#[tokio::test]
async fn servers_share_the_count_and_the_grace_period() {
    away_from_hour_boundary().await;
    let mut account = NewAccount::free("two servers");
    account.rate_limit_per_sec = None;
    let app = TestApp::spawn_with(account).await;
    let path = format!("/v1/tenants/{}/wallet", unique_tenant("cus_shared"));
    let server = || {
        let state = AppState::new(app.pool.clone());
        let api = Api { router: router(state.clone()), api_key: app.api.api_key.clone() };
        (state, api)
    };

    // Server C caches the account before the grace period starts.
    let (state_c, api_c) = server();
    assert_eq!(quota(&send(&api_c, Method::GET, &path, None, None).await), (120, 119, None));

    // Server A takes the account over its limit.
    for _ in 0..120 {
        app.get(&path).await;
    }
    let res = app.get(&path).await;
    let ends = grace_ends_at(&app).await.expect("grace period started");
    assert_eq!(quota(&res), (600, 600 - 121, Some(ends.timestamp())));
    app.state.usage.flush(&app.pool).await.unwrap();

    // A new server B sees both the count and the grace period.
    let (_, api_b) = server();
    let res = send(&api_b, Method::GET, &path, None, None).await;
    assert_eq!(quota(&res), (600, 600 - 122, Some(ends.timestamp())), "121 flushed by A, plus this one");

    // Server C still has the old cached account; once its count catches up
    // it finds the grace period A started rather than starting a new one.
    state_c.usage.flush(&app.pool).await.unwrap();
    let res = send(&api_c, Method::GET, &path, None, None).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
    assert_eq!(quota(&res).2, Some(ends.timestamp()));
    assert_eq!(grace_ends_at(&app).await, Some(ends));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn hourly_limit_is_exact_under_concurrency() {
    away_from_hour_boundary().await;
    let mut account = NewAccount::free("concurrent");
    account.rate_limit_per_sec = None;
    account.hourly_request_limit = Some(50);
    account.grace_hourly_request_limit = Some(100);
    let app = TestApp::spawn_with(account).await;
    let path = Arc::new(format!("/v1/tenants/{}/wallet", unique_tenant("cus_parallel")));

    const TASKS: usize = 16;
    const PER_TASK: usize = 10;
    let barrier = Arc::new(Barrier::new(TASKS));
    let mut handles = Vec::new();
    for _ in 0..TASKS {
        let (api, barrier, path) = (app.api.clone(), barrier.clone(), path.clone());
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut statuses = Vec::new();
            for _ in 0..PER_TASK {
                statuses.push(send(&api, Method::GET, &path, None, None).await);
            }
            statuses
        }));
    }
    let mut admitted = 0;
    let mut grace_headers = std::collections::HashSet::new();
    for h in handles {
        for res in h.await.unwrap() {
            match res.status {
                StatusCode::NOT_FOUND => admitted += 1,
                StatusCode::TOO_MANY_REQUESTS => assert_eq!(res.body["error"], "hourly_limit_exceeded"),
                other => panic!("unexpected {other}: {}", res.body),
            }
            if let Some(ends) = header_num(&res, HEADER_GRACE_ENDS) {
                grace_headers.insert(ends);
            }
        }
    }
    assert_eq!(admitted, 100, "exactly the grace limit");
    let ends = grace_ends_at(&app).await.unwrap();
    assert_eq!(grace_headers.into_iter().collect::<Vec<_>>(), vec![ends.timestamp()], "one grace period");
    app.state.usage.flush(&app.pool).await.unwrap();
    assert_eq!(hourly_count(&app).await, 100);
}
