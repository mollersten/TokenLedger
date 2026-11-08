//! Authentication, account isolation and trial-limit enforcement.

mod common;

use std::{sync::Arc, time::Duration};

use axum::http::{Method, StatusCode, header};
use backend::accounts::{self, LimitChange, NewAccount};
use chrono::Utc;
use common::{TestApp, balances, new_key, post, reservation_id, send, test_pool, unique_tenant};
use serde_json::json;
use tokio::sync::Barrier;

fn unlimited() -> NewAccount {
    NewAccount::unlimited("auth test")
}

#[tokio::test]
async fn requests_without_a_valid_key_are_rejected() {
    let app = TestApp::spawn().await;
    let path = "/v1/tenants/cus_x/wallet";

    let res = send(&app.api.with_key(None), Method::GET, path, None, None).await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
    assert_eq!(res.body["error"], "missing_api_key");
    assert_eq!(res.headers.get(header::WWW_AUTHENTICATE).unwrap(), "Bearer");

    for bad in ["not-a-key", "tl_live_short", &accounts::generate_secret()] {
        let res = send(&app.api.with_key(Some(bad)), Method::GET, path, None, None).await;
        assert_eq!(res.status, StatusCode::UNAUTHORIZED, "key {bad:?}");
        assert_eq!(res.body["error"], "invalid_api_key");
    }

    // Mutations are rejected before any state change.
    let tenant = unique_tenant("cus_noauth");
    let res = post(&app.api.with_key(None), "/v1/credits/grant", &new_key(), json!({ "tenant_id": tenant, "amount": 5 })).await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
    let wallets: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM wallets WHERE tenant_id = $1")
        .bind(&tenant)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(wallets, 0);

    let health = send(&app.api.with_key(None), Method::GET, "/healthz", None, None).await;
    assert_eq!(health.status, StatusCode::OK, "health check stays public");
}

#[tokio::test]
async fn accounts_are_fully_isolated() {
    let app = TestApp::spawn().await;
    let (other_id, other) = app.second_account(unlimited()).await;
    // Deliberately the same tenant id and idempotency key in both accounts.
    let tenant = "cus_shared";
    let key = new_key();

    let a = app.post("/v1/credits/grant", &key, json!({ "tenant_id": tenant, "amount": 100 })).await;
    let b = post(&other, "/v1/credits/grant", &key, json!({ "tenant_id": tenant, "amount": 7 })).await;
    assert_eq!((a.status, b.status), (StatusCode::OK, StatusCode::OK));
    assert!(!a.replayed && !b.replayed, "idempotency keys are scoped per account");
    assert_eq!(app.balances(tenant).await, (100, 0));
    assert_eq!(balances(&app.pool, &other_id, tenant).await, (7, 0));

    let theirs = send(&other, Method::GET, &format!("/v1/tenants/{tenant}/wallet"), None, None).await;
    assert_eq!(theirs.body["available_credits"], 7);
    assert_eq!(theirs.body["recent_ledger"].as_array().unwrap().len(), 1);

    // A reservation id from account A means nothing to account B.
    let id = reservation_id(&app.reserve(tenant, 10, 300).await);
    for path in ["/v1/credits/commit", "/v1/credits/release"] {
        let res = post(
            &other,
            path,
            &new_key(),
            json!({ "tenant_id": tenant, "reservation_id": id, "actual_credits_used": 1 }),
        )
        .await;
        assert_eq!(res.status, StatusCode::NOT_FOUND, "{path}: {}", res.body);
        assert_eq!(res.body["error"], "reservation_not_found");
    }
    assert_eq!(app.reservation(id).await.0, "PENDING");
    assert_eq!(app.balances(tenant).await, (90, 10));
    assert_eq!(balances(&app.pool, &other_id, tenant).await, (7, 0));

    // A tenant that exists only in account A is unknown to account B.
    let only_a = unique_tenant("cus_only_a");
    app.grant(&only_a, 5).await;
    let res = send(&other, Method::GET, &format!("/v1/tenants/{only_a}/wallet"), None, None).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn revoked_keys_stop_working() {
    let app = TestApp::spawn_full(unlimited(), Duration::ZERO).await;
    let tenant = unique_tenant("cus_revoke");
    app.grant(&tenant, 5).await;

    assert!(accounts::revoke_key(&app.pool, &app.key_id).await.unwrap());
    let res = app.grant_raw(&tenant, 5).await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
    assert_eq!(res.body["error"], "invalid_api_key");

    let fresh = accounts::issue_key(&app.pool, &app.account_id, None).await.unwrap();
    let res = post(
        &app.api.with_key(Some(&fresh.secret)),
        "/v1/credits/grant",
        &new_key(),
        json!({ "tenant_id": tenant, "amount": 5 }),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(app.balances(&tenant).await, (10, 0));
}

#[tokio::test]
async fn suspended_accounts_and_expired_trials_are_refused() {
    let app = TestApp::spawn_full(NewAccount::trial("trial test"), Duration::ZERO).await;
    let tenant = unique_tenant("cus_trial");
    app.grant(&tenant, 5).await;

    accounts::set_status(&app.pool, &app.account_id, false).await.unwrap();
    let res = app.grant_raw(&tenant, 5).await;
    assert_eq!(res.status, StatusCode::FORBIDDEN);
    assert_eq!(res.body["error"], "account_suspended");
    accounts::set_status(&app.pool, &app.account_id, true).await.unwrap();

    accounts::set_trial_end(&app.pool, &app.account_id, Some(Utc::now() - chrono::Duration::minutes(1)))
        .await
        .unwrap();
    let res = app.get(&format!("/v1/tenants/{tenant}/wallet")).await;
    assert_eq!(res.status, StatusCode::FORBIDDEN);
    assert_eq!(res.body["error"], "trial_expired");

    accounts::extend_trial(&app.pool, &app.account_id, 7).await.unwrap();
    assert_eq!(app.grant_raw(&tenant, 5).await.status, StatusCode::OK);

    // Converting to paid removes the trial window entirely.
    accounts::set_trial_end(&app.pool, &app.account_id, Some(Utc::now() - chrono::Duration::minutes(1)))
        .await
        .unwrap();
    accounts::set_plan(&app.pool, &app.account_id, accounts::Plan::Paid).await.unwrap();
    assert_eq!(app.grant_raw(&tenant, 5).await.status, StatusCode::OK);
    assert_eq!(app.balances(&tenant).await, (15, 0));
}

#[tokio::test]
async fn per_key_rate_limit_returns_429_with_retry_after() {
    let mut account = unlimited();
    account.rate_limit_per_sec = Some(5);
    let app = TestApp::spawn_with(account).await;
    let path = format!("/v1/tenants/{}/wallet", unique_tenant("cus_rate"));

    let mut allowed = 0;
    let mut limited = Vec::new();
    for _ in 0..12 {
        let res = app.get(&path).await;
        match res.status {
            StatusCode::NOT_FOUND => allowed += 1, // authenticated, wallet simply missing
            StatusCode::TOO_MANY_REQUESTS => limited.push(res),
            other => panic!("unexpected {other}: {}", res.body),
        }
    }
    assert!((5..=6).contains(&allowed), "burst should be ~5, got {allowed}");
    let first = &limited[0];
    assert_eq!(first.body["error"], "rate_limited");
    assert_eq!(first.headers.get(header::RETRY_AFTER).unwrap(), "1");

    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(app.get(&path).await.status, StatusCode::NOT_FOUND, "tokens refill");
}

#[tokio::test]
async fn monthly_quota_is_enforced_and_persisted() {
    let mut account = unlimited();
    account.monthly_request_limit = Some(3);
    let app = TestApp::spawn_with(account).await;
    let tenant = unique_tenant("cus_quota");

    app.grant(&tenant, 1).await;
    app.grant(&tenant, 1).await;
    assert_eq!(app.get(&format!("/v1/tenants/{tenant}/wallet")).await.status, StatusCode::OK);
    let res = app.grant_raw(&tenant, 1).await;
    assert_eq!(res.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(res.body["error"], "monthly_quota_exceeded");
    let retry: u64 = res.headers.get(header::RETRY_AFTER).unwrap().to_str().unwrap().parse().unwrap();
    assert!(retry > 0 && retry <= 31 * 86_400, "retry after the month resets: {retry}");
    assert_eq!(app.balances(&tenant).await, (2, 0), "the rejected grant did nothing");

    assert_eq!(app.state.usage.flush(&app.pool).await.unwrap(), 1);
    let recorded: i64 = sqlx::query_scalar("SELECT request_count FROM account_usage WHERE account_id = $1")
        .bind(&app.account_id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(recorded, 3, "only accepted requests are counted");

    // A new server process picks the count up from the database.
    let restarted = backend::router(backend::AppState::new(test_pool().await));
    let api = common::Api { router: restarted, api_key: app.api.api_key.clone() };
    let res = send(&api, Method::GET, &format!("/v1/tenants/{tenant}/wallet"), None, None).await;
    assert_eq!(res.status, StatusCode::TOO_MANY_REQUESTS);

    // Raising the limit lets traffic through again.
    let change = LimitChange { monthly_request_limit: Some(Some(10)), ..LimitChange::default() };
    accounts::set_limits(&app.pool, &app.account_id, &change).await.unwrap();
    let fresh = backend::router(backend::AppState::new(test_pool().await));
    let api = common::Api { router: fresh, api_key: app.api.api_key.clone() };
    let res = send(&api, Method::GET, &format!("/v1/tenants/{tenant}/wallet"), None, None).await;
    assert_eq!(res.status, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn tenant_limit_holds_under_concurrent_first_grants() {
    let mut account = unlimited();
    account.max_tenants = Some(2);
    let app = TestApp::spawn_with(account).await;

    const TASKS: usize = 6;
    let barrier = Arc::new(Barrier::new(TASKS));
    let mut handles = Vec::new();
    for i in 0..TASKS {
        let (api, barrier) = (app.api.clone(), barrier.clone());
        handles.push(tokio::spawn(async move {
            let body = json!({ "tenant_id": format!("cus_limit_{i}"), "amount": 10 });
            barrier.wait().await;
            post(&api, "/v1/credits/grant", &new_key(), body).await
        }));
    }
    let mut ok = Vec::new();
    for (i, h) in handles.into_iter().enumerate() {
        let res = h.await.unwrap();
        match res.status {
            StatusCode::OK => ok.push(format!("cus_limit_{i}")),
            StatusCode::FORBIDDEN => assert_eq!(res.body["error"], "tenant_limit_reached"),
            other => panic!("unexpected {other}: {}", res.body),
        }
    }
    assert_eq!(ok.len(), 2, "exactly max_tenants wallets may be created");
    let wallets: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM wallets WHERE account_id = $1")
        .bind(&app.account_id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(wallets, 2);

    // Existing tenants keep working at the limit.
    assert_eq!(app.grant_raw(&ok[0], 5).await.status, StatusCode::OK);
    assert_eq!(app.balances(&ok[0]).await, (15, 0));
}
