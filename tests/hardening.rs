//! Production hardening: overload answers 503 instead of starving other
//! accounts, requests are strict about their fields and sizes, and plan
//! changes behave.

mod common;

use std::time::Duration;

use axum::http::{Method, StatusCode};
use backend::{accounts, models::MAX_CREDITS};
use common::{TestApp, new_key, reservation_id, send, unique_tenant};
use serde_json::json;

#[tokio::test]
async fn requests_for_a_busy_wallet_wait_without_a_connection_then_proceed() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_gate");
    app.grant(&tenant, 10).await;
    // Two requests already in flight for this wallet: a third waits at the gate.
    let p1 = app.state.gates.wallet(&app.account_id, &tenant).await.unwrap();
    let p2 = app.state.gates.wallet(&app.account_id, &tenant).await.unwrap();
    let api = app.api.clone();
    let t = tenant.clone();
    let waiting = tokio::spawn(async move {
        common::post(&api, "/v1/credits/grant", &new_key(), json!({ "tenant_id": t, "amount": 5 })).await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!waiting.is_finished(), "must wait while the wallet's gate is full");
    drop((p1, p2));
    let res = waiting.await.unwrap();
    assert_eq!(res.status, StatusCode::OK, "{}", res.body);
    assert_eq!(app.balances(&tenant).await, (15, 0));
}

#[tokio::test]
async fn a_lock_held_too_long_answers_503_instead_of_hanging() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_lock");
    app.grant(&tenant, 10).await;
    let held = app.hold_wallet_lock(&tenant).await;
    let res = app.reserve(&tenant, 1, 60).await;
    assert_eq!(res.status, StatusCode::SERVICE_UNAVAILABLE, "{}", res.body);
    assert_eq!(res.body["error"], "wallet_busy");
    assert!(res.headers.contains_key("retry-after"));
    held.rollback().await.unwrap();
    // Nothing was written, and the next attempt goes through.
    assert_eq!(app.balances(&tenant).await, (10, 0));
    assert_eq!(app.reserve(&tenant, 1, 60).await.status, StatusCode::OK);
}

#[tokio::test]
async fn a_duplicate_item_reserve_queued_behind_the_lock_gets_the_first_answer() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_dup_item");
    app.grant(&tenant, 10).await;
    let held = app.hold_wallet_lock(&tenant).await;
    let key = new_key();
    let body = json!({ "tenant_id": tenant, "feature": "email_lookup", "estimate_credits": 1, "item_key": "email:1" });
    let (a, b) = (app.api.clone(), app.api.clone());
    let (ka, kb, ba, bb) = (key.clone(), key.clone(), body.clone(), body);
    let first = tokio::spawn(async move { common::post(&a, "/v1/credits/reserve", &ka, ba).await });
    let second = tokio::spawn(async move { common::post(&b, "/v1/credits/reserve", &kb, bb).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    held.rollback().await.unwrap();
    let (first, second) = (first.await.unwrap(), second.await.unwrap());
    assert_eq!((first.status, second.status), (StatusCode::OK, StatusCode::OK), "{} / {}", first.body, second.body);
    assert_eq!(reservation_id(&first), reservation_id(&second));
    assert!(first.replayed || second.replayed, "one of them must be the replay");
    assert_eq!(app.balances(&tenant).await, (9, 1));
}

#[tokio::test]
async fn misspelt_fields_are_refused_instead_of_ignored() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_strict");
    app.grant(&tenant, 10).await;
    let reserve = app
        .post("/v1/credits/reserve", &new_key(), json!({ "tenant_id": tenant, "feature": "f", "estimate_credits": 1, "itemKey": "email:1" }))
        .await;
    assert_eq!(reserve.status, StatusCode::BAD_REQUEST, "{}", reserve.body);
    let grant = app
        .post("/v1/credits/grant", &new_key(), json!({ "tenant_id": tenant, "amount": 5, "expiresAt": "2099-01-01T00:00:00Z" }))
        .await;
    assert_eq!(grant.status, StatusCode::BAD_REQUEST, "{}", grant.body);
    assert_eq!(app.balances(&tenant).await, (10, 0));
}

#[tokio::test]
async fn metadata_and_amounts_are_bounded() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_bounds");
    app.grant(&tenant, 10).await;
    let big = "x".repeat(20 * 1024);
    let res = app
        .post("/v1/credits/reserve", &new_key(), json!({ "tenant_id": tenant, "feature": "f", "estimate_credits": 1, "metadata": { "blob": big } }))
        .await;
    assert_eq!((res.status, res.body["error"].clone()), (StatusCode::BAD_REQUEST, json!("invalid_metadata")));

    let too_big = app.post("/v1/credits/grant", &new_key(), json!({ "tenant_id": tenant, "amount": MAX_CREDITS + 1 })).await;
    assert_eq!((too_big.status, too_big.body["error"].clone()), (StatusCode::BAD_REQUEST, json!("invalid_amount")));
    // Up to the largest exact JavaScript integer, and not past it in total.
    let over = app.post("/v1/credits/grant", &new_key(), json!({ "tenant_id": tenant, "amount": MAX_CREDITS })).await;
    assert_eq!((over.status, over.body["error"].clone()), (StatusCode::UNPROCESSABLE_ENTITY, json!("balance_overflow")));
    assert_eq!(app.balances(&tenant).await, (10, 0));
}

#[tokio::test]
async fn moving_an_account_to_the_trial_plan_gives_it_a_trial_window() {
    let app = TestApp::spawn().await;
    assert!(accounts::set_plan(&app.pool, &app.account_id, accounts::Plan::Trial, false).await.unwrap());
    let account = accounts::get_account(&app.pool, &app.account_id).await.unwrap().unwrap();
    let ends = account.trial_ends_at.expect("a trial must have an end");
    let days = (ends - chrono::Utc::now()).num_days();
    assert!((13..=14).contains(&days), "trial ends in {days} days");
}

#[tokio::test]
async fn liveness_does_not_need_a_key_or_the_database() {
    let app = TestApp::spawn().await;
    let res = send(&app.api.with_key(None), Method::GET, "/livez", None, None).await;
    assert_eq!(res.status, StatusCode::OK);
}

#[tokio::test]
async fn idempotency_records_older_than_the_retention_are_pruned() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_prune");
    let (old_key, new_key_) = (new_key(), new_key());
    app.post("/v1/credits/grant", &old_key, json!({ "tenant_id": tenant, "amount": 5 })).await;
    app.post("/v1/credits/grant", &new_key_, json!({ "tenant_id": tenant, "amount": 5 })).await;
    sqlx::query("UPDATE idempotency_keys SET created_at = NOW() - INTERVAL '25 hours' WHERE idempotency_key = $1")
        .bind(&old_key)
        .execute(&app.pool)
        .await
        .unwrap();
    backend::idempotency::prune(&app.pool, backend::idempotency::DEFAULT_RETENTION).await.unwrap();
    let left: Vec<String> = sqlx::query_scalar("SELECT idempotency_key FROM idempotency_keys WHERE account_id = $1")
        .bind(&app.account_id)
        .fetch_all(&app.pool)
        .await
        .unwrap();
    assert_eq!(left, vec![new_key_]);
}

#[tokio::test]
async fn nul_characters_in_metadata_are_refused_not_a_server_error() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_nul_meta");
    app.grant(&tenant, 10).await;
    for metadata in [json!({ "note": "a\u{0}b" }), json!({ "a\u{0}": 1 }), json!({ "nested": [{ "x": "\u{0}" }] })] {
        let res = app
            .post("/v1/credits/reserve", &new_key(), json!({ "tenant_id": tenant, "feature": "f", "estimate_credits": 1, "metadata": metadata }))
            .await;
        assert_eq!((res.status, res.body["error"].clone()), (StatusCode::BAD_REQUEST, json!("invalid_metadata")), "{metadata}");
    }
    assert_eq!(app.balances(&tenant).await, (10, 0));
}
