//! Charging once per item (`item_key` on reserve): a tenant pays for an item
//! once, and none of the ways an end user can steer requests (retrying after
//! a failure, racing requests, reusing a key across tenants or features)
//! gets an item free or charges it twice.

mod common;

use std::{sync::Arc, time::Duration};

use axum::http::{StatusCode, header};
use common::{TestApp, TestResponse, new_key, post, reservation_id, unique_tenant};
use serde_json::{Value, json};
use tokio::sync::Barrier;

const ITEM: &str = "email:person_42";
const FEATURE: &str = "email_lookup";

fn item_body(tenant: &str, feature: &str, item_key: &str, estimate: i64, ttl_seconds: i64) -> Value {
    json!({
        "tenant_id": tenant,
        "feature": feature,
        "estimate_credits": estimate,
        "ttl_seconds": ttl_seconds,
        "item_key": item_key,
    })
}

async fn reserve_item(app: &TestApp, tenant: &str, key: &str) -> TestResponse {
    app.post("/v1/credits/reserve", key, item_body(tenant, FEATURE, ITEM, 5, 60)).await
}

/// Reserve and commit the item; returns the reservation id.
async fn buy_item(app: &TestApp, tenant: &str) -> uuid::Uuid {
    let res = reserve_item(app, tenant, &new_key()).await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.body);
    assert_eq!(res.body["already_charged"], false);
    assert_eq!(res.body["item_key"], ITEM);
    let id = reservation_id(&res);
    let commit = app.commit(tenant, id, 5, &new_key()).await;
    assert_eq!(commit.status, StatusCode::OK, "{}", commit.body);
    id
}

#[tokio::test]
async fn an_item_is_charged_once_and_then_answers_already_charged() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_item");
    app.grant(&tenant, 100).await;
    let paid = buy_item(&app, &tenant).await;
    assert_eq!(app.balances(&tenant).await, (95, 0));
    let entries = app.count_ledger(&tenant, "RESERVE").await;

    // The tenant's code crashed after getting the item and asks again, with a
    // new idempotency key (a new process): nothing more is charged.
    for _ in 0..3 {
        let again = reserve_item(&app, &tenant, &new_key()).await;
        assert_eq!(again.status, StatusCode::OK, "{}", again.body);
        assert_eq!(again.body["allowed"], true);
        assert_eq!(again.body["already_charged"], true);
        assert_eq!(again.body["reservation_id"], json!(paid));
        assert_eq!(again.body["charged_credits"], 5);
        assert_eq!(again.body["remaining_available"], 95);
        assert!(!again.replayed, "a fresh key must be decided, not replayed");
    }
    assert_eq!(app.balances(&tenant).await, (95, 0));
    assert_eq!(app.count_reservations(&tenant).await, 1);
    assert_eq!(app.count_ledger(&tenant, "RESERVE").await, entries);
    app.assert_ledger_reconciles(&tenant).await;
}

#[tokio::test]
async fn paying_for_an_item_does_not_unlock_it_for_another_tenant() {
    let app = TestApp::spawn().await;
    let (alice, bob) = (unique_tenant("cus_alice"), unique_tenant("cus_bob"));
    app.grant(&alice, 100).await;
    app.grant(&bob, 100).await;
    buy_item(&app, &alice).await;

    let res = reserve_item(&app, &bob, &new_key()).await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.body);
    assert_eq!(res.body["already_charged"], false, "bob has not paid for it");
    assert_eq!(app.balances(&bob).await, (95, 5));
}

#[tokio::test]
async fn a_failed_attempt_does_not_unlock_the_item() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_item_fail");
    app.grant(&tenant, 100).await;

    // First attempt fails (the work threw, or the end user cancelled) and is released.
    let first = reserve_item(&app, &tenant, &new_key()).await;
    let first_id = reservation_id(&first);
    assert_eq!(app.release(&tenant, first_id, &new_key()).await.status, StatusCode::OK);
    assert_eq!(app.balances(&tenant).await, (100, 0));

    // Retrying is not free: it is a new charge.
    let retry = reserve_item(&app, &tenant, &new_key()).await;
    assert_eq!(retry.status, StatusCode::OK, "{}", retry.body);
    assert_eq!(retry.body["already_charged"], false);
    assert_ne!(reservation_id(&retry), first_id);
    assert_eq!(app.balances(&tenant).await, (95, 5));
    app.assert_ledger_reconciles(&tenant).await;
}

#[tokio::test]
async fn an_expired_hold_can_neither_be_committed_late_nor_unlock_the_item() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_item_ttl");
    app.grant(&tenant, 100).await;

    let first = app.post("/v1/credits/reserve", &new_key(), item_body(&tenant, FEATURE, ITEM, 5, 1)).await;
    assert_eq!(first.status, StatusCode::OK, "{}", first.body);
    let first_id = reservation_id(&first);
    tokio::time::sleep(Duration::from_millis(2_100)).await;

    // Past its TTL the old hold is returned and the item is charged anew.
    let retry = reserve_item(&app, &tenant, &new_key()).await;
    assert_eq!(retry.status, StatusCode::OK, "{}", retry.body);
    assert_eq!(retry.body["already_charged"], false);
    assert_eq!(app.reservation(first_id).await.0, "EXPIRED");
    assert_eq!(app.balances(&tenant).await, (95, 5));

    // The slow first attempt finishing now cannot commit and mark it paid.
    let late = app.commit(&tenant, first_id, 5, &new_key()).await;
    assert_eq!(late.status, StatusCode::CONFLICT, "{}", late.body);
    assert_eq!(late.body["error"], "reservation_not_pending");
    app.assert_ledger_reconciles(&tenant).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn racing_requests_for_one_item_charge_it_once() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_item_race");
    app.grant(&tenant, 1_000).await;

    const TASKS: usize = 20;
    let barrier = Arc::new(Barrier::new(TASKS));
    let mut handles = Vec::with_capacity(TASKS);
    for _ in 0..TASKS {
        let api = app.api.clone();
        let barrier = barrier.clone();
        let body = item_body(&tenant, FEATURE, ITEM, 5, 60);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            post(&api, "/v1/credits/reserve", &new_key(), body).await
        }));
    }
    let mut holds = Vec::new();
    let mut in_progress = 0;
    for handle in handles {
        let res = handle.await.expect("task panicked");
        match res.status {
            StatusCode::OK => holds.push(res),
            StatusCode::CONFLICT => {
                assert_eq!(res.body["error"], "item_charge_in_progress", "{}", res.body);
                assert!(res.headers.contains_key(header::RETRY_AFTER));
                in_progress += 1;
            }
            other => panic!("unexpected status {other}: {}", res.body),
        }
    }
    assert_eq!(holds.len(), 1, "exactly one request may charge the item");
    assert_eq!(in_progress, TASKS - 1);
    assert_eq!(app.balances(&tenant).await, (995, 5));

    // Once it is paid, everyone who asks gets it without paying again.
    let id = reservation_id(&holds[0]);
    assert_eq!(app.commit(&tenant, id, 5, &new_key()).await.status, StatusCode::OK);
    let after = reserve_item(&app, &tenant, &new_key()).await;
    assert_eq!(after.body["already_charged"], true, "{}", after.body);
    assert_eq!(app.balances(&tenant).await, (995, 0));
    assert_eq!(app.count_reservations(&tenant).await, 1);
    app.assert_ledger_reconciles(&tenant).await;
}

#[tokio::test]
async fn in_progress_is_not_cached_under_the_idempotency_key() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_item_retry");
    app.grant(&tenant, 100).await;
    let first = reserve_item(&app, &tenant, &new_key()).await;
    let id = reservation_id(&first);

    let key = new_key();
    let busy = reserve_item(&app, &tenant, &key).await;
    assert_eq!(busy.status, StatusCode::CONFLICT, "{}", busy.body);

    // The SDK retries with the same key; after the commit it must see the
    // new state, not a stored copy of the conflict.
    assert_eq!(app.commit(&tenant, id, 5, &new_key()).await.status, StatusCode::OK);
    let retried = reserve_item(&app, &tenant, &key).await;
    assert_eq!(retried.status, StatusCode::OK, "{}", retried.body);
    assert_eq!(retried.body["already_charged"], true);
}

#[tokio::test]
async fn the_same_key_under_another_feature_is_another_item() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_item_feature");
    app.grant(&tenant, 100).await;
    buy_item(&app, &tenant).await;

    // Owning the email does not unlock the phone number: that is bought separately.
    let other = app
        .post("/v1/credits/reserve", &new_key(), item_body(&tenant, "phone_lookup", ITEM, 20, 60))
        .await;
    assert_eq!(other.status, StatusCode::OK, "{}", other.body);
    assert_eq!(other.body["already_charged"], false);
    assert_eq!(app.balances(&tenant).await, (75, 20));
}

#[tokio::test]
async fn an_item_is_stored_once_and_ownership_is_a_small_row_per_wallet() {
    let app = TestApp::spawn().await;
    let (a, b) = (unique_tenant("cus_own_a"), unique_tenant("cus_own_b"));
    app.grant(&a, 100).await;
    app.grant(&b, 100).await;
    buy_item(&app, &a).await;
    buy_item(&app, &b).await;
    let items: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM items WHERE account_id = $1 AND item_key = $2")
        .bind(&app.account_id)
        .bind(ITEM)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(items, 1, "one item row however many tenants buy it");
    assert_eq!((app.count_owned_items(&a).await, app.count_owned_items(&b).await), (1, 1));
}

#[tokio::test]
async fn owned_items_expire_after_the_accounts_retention_and_are_pruned() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_item_retention");
    app.grant(&tenant, 100).await;
    let settings = app.patch("/v1/settings", json!({ "item_retention_days": 30 })).await;
    assert_eq!(settings.body["item_retention_days"], 30, "{}", settings.body);
    buy_item(&app, &tenant).await;
    assert_eq!(reserve_item(&app, &tenant, &new_key()).await.body["already_charged"], true);

    // Bought 31 days ago: no longer owned, so it is charged again, and the
    // purchase is renewed rather than duplicated.
    sqlx::query("UPDATE wallets_items SET created_at = NOW() - INTERVAL '31 days' WHERE wallet_id = (SELECT id FROM wallets WHERE account_id = $1 AND tenant_id = $2)")
        .bind(&app.account_id).bind(&tenant).execute(&app.pool).await.unwrap();
    buy_item(&app, &tenant).await;
    assert_eq!(app.balances(&tenant).await, (90, 0));
    assert_eq!(app.count_owned_items(&tenant).await, 1);
    assert_eq!(reserve_item(&app, &tenant, &new_key()).await.body["already_charged"], true);

    // Expired rows are kept for a grace period, so a retention set too short
    // can be raised again and the items are owned once more.
    let age = |days: i32| {
        sqlx::query("UPDATE wallets_items SET created_at = NOW() - make_interval(days => $3) \
                     WHERE wallet_id = (SELECT id FROM wallets WHERE account_id = $1 AND tenant_id = $2)")
            .bind(app.account_id.clone()).bind(tenant.clone()).bind(days)
    };
    age(31).execute(&app.pool).await.unwrap();
    backend::ledger::prune_expired_items(&app.pool).await.unwrap();
    assert_eq!(app.count_owned_items(&tenant).await, 1, "still inside the grace period");
    assert_eq!(reserve_item(&app, &tenant, &new_key()).await.body["already_charged"], false, "but expired");
    app.patch("/v1/settings", json!({ "item_retention_days": 60 })).await;
    assert_eq!(reserve_item(&app, &tenant, &new_key()).await.body["already_charged"], true, "owned again");

    // Past retention plus the grace period, the background job deletes it.
    app.patch("/v1/settings", json!({ "item_retention_days": 30 })).await;
    age(30 + backend::ledger::ITEM_DELETE_GRACE_DAYS + 1).execute(&app.pool).await.unwrap();
    backend::ledger::prune_expired_items(&app.pool).await.unwrap();
    assert_eq!(app.count_owned_items(&tenant).await, 0);

    // Back to forever.
    let forever = app.patch("/v1/settings", json!({ "item_retention_days": null })).await;
    assert_eq!(forever.body["item_retention_days"], json!(null));
}

#[tokio::test]
async fn settings_are_validated() {
    let app = TestApp::spawn().await;
    assert_eq!(app.get("/v1/settings").await.body["item_retention_days"], json!(null));
    for body in [json!({}), json!({ "item_retention_days": 0 }), json!({ "item_retention_days": -3 }),
                 json!({ "item_retention_days": "90" }), json!({ "item_retention_days": 90, "x": 1 })] {
        let res = app.patch("/v1/settings", body.clone()).await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST, "{body}: {}", res.body);
    }
}

#[tokio::test]
async fn an_item_cannot_be_committed_for_zero_credits() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_item_zero");
    app.grant(&tenant, 100).await;
    let res = reserve_item(&app, &tenant, &new_key()).await;
    let id = reservation_id(&res);

    let zero = app.commit(&tenant, id, 0, &new_key()).await;
    assert_eq!(zero.status, StatusCode::UNPROCESSABLE_ENTITY, "{}", zero.body);
    assert_eq!(zero.body["error"], "invalid_actual_credits_used");
    assert_eq!(app.reservation(id).await, ("PENDING".to_owned(), None));

    // Releasing charges nothing and leaves the item unpaid.
    assert_eq!(app.release(&tenant, id, &new_key()).await.status, StatusCode::OK);
    let again = reserve_item(&app, &tenant, &new_key()).await;
    assert_eq!(again.body["already_charged"], false, "{}", again.body);

    // Reservations without an item key may still commit 0.
    let plain = app.reserve(&tenant, 5, 60).await;
    let plain_commit = app.commit(&tenant, reservation_id(&plain), 0, &new_key()).await;
    assert_eq!(plain_commit.status, StatusCode::OK, "{}", plain_commit.body);
    app.assert_ledger_reconciles(&tenant).await;
}

#[tokio::test]
async fn the_database_refuses_a_second_live_reservation_for_an_item() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_item_index");
    app.grant(&tenant, 100).await;
    let res = reserve_item(&app, &tenant, &new_key()).await;
    let id = reservation_id(&res);

    // Bypass the handler: the partial unique index is the backstop.
    let duplicate = sqlx::query(
        "INSERT INTO reservations (id, account_id, tenant_id, feature, reserved_amount, status, \
                                   expires_at, item_id) \
         SELECT gen_random_uuid(), account_id, tenant_id, feature, 1, 'PENDING', expires_at, item_id \
         FROM reservations WHERE id = $1",
    )
    .bind(id)
    .execute(&app.pool)
    .await;
    let err = duplicate.expect_err("a second PENDING reservation for the item must be refused");
    assert!(err.to_string().contains("reservations_pending_item_idx"), "{err}");

    // And a wallet cannot own the same item twice.
    assert_eq!(app.commit(&tenant, id, 5, &new_key()).await.status, StatusCode::OK);
    let twice = sqlx::query(
        "INSERT INTO wallets_items (wallet_id, item_id, credits_paid) \
         SELECT wi.wallet_id, wi.item_id, 1 FROM wallets_items wi JOIN reservations r ON r.id = wi.reservation_id \
         WHERE r.id = $1",
    )
    .bind(id)
    .execute(&app.pool)
    .await;
    assert!(twice.expect_err("owning an item twice must be refused").to_string().contains("wallets_items_pkey"));
}

#[tokio::test]
async fn item_keys_are_validated() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_item_valid");
    app.grant(&tenant, 100).await;
    for bad in ["", "   ", "has\nnewline", &"x".repeat(256)] {
        let res = app.post("/v1/credits/reserve", &new_key(), item_body(&tenant, FEATURE, bad, 5, 60)).await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST, "{bad:?}: {}", res.body);
        assert_eq!(res.body["error"], "invalid_item_key");
    }
    assert_eq!(app.balances(&tenant).await, (100, 0));
}

#[tokio::test]
async fn after_the_reaper_returns_a_stuck_hold_the_item_can_be_charged() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_item_reaper");
    app.grant(&tenant, 100).await;
    let stuck = app.post("/v1/credits/reserve", &new_key(), item_body(&tenant, FEATURE, ITEM, 5, 1)).await;
    let stuck_id = reservation_id(&stuck);
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    backend::reaper::run_once(&app.pool, backend::reaper::BATCH_SIZE).await.unwrap();
    assert_eq!(app.reservation(stuck_id).await.0, "EXPIRED");
    assert_eq!(app.balances(&tenant).await, (100, 0));

    let paid = buy_item(&app, &tenant).await;
    assert_ne!(paid, stuck_id);
    assert_eq!(app.balances(&tenant).await, (95, 0));
    app.assert_ledger_reconciles(&tenant).await;
}

#[tokio::test]
async fn expiring_a_stale_item_hold_returns_its_expiring_credits_to_their_bucket() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_item_bucket");
    let bucket = app.grant_expiring(&tenant, 10, chrono::Utc::now() + chrono::Duration::days(30)).await;
    let stale = app.post("/v1/credits/reserve", &new_key(), item_body(&tenant, FEATURE, ITEM, 5, 1)).await;
    assert_eq!(stale.status, StatusCode::OK, "{}", stale.body);
    assert_eq!(app.bucket_remaining(bucket).await, 5);
    tokio::time::sleep(Duration::from_millis(2_100)).await;

    // The retry expires the stale hold (credits back in the bucket) and holds anew.
    let retry = reserve_item(&app, &tenant, &new_key()).await;
    assert_eq!(retry.status, StatusCode::OK, "{}", retry.body);
    assert_eq!(app.reservation(reservation_id(&stale)).await.0, "EXPIRED");
    assert_eq!(app.bucket_remaining(bucket).await, 5);
    assert_eq!(app.wallet_state(&tenant).await, (5, 5, 5));
    app.assert_buckets_reconcile(&tenant).await;
    app.assert_ledger_reconciles(&tenant).await;
}

#[tokio::test]
async fn a_reserve_replayed_after_the_item_was_paid_still_cannot_charge_twice() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_item_replay");
    app.grant(&tenant, 100).await;
    let key = new_key();
    let first = reserve_item(&app, &tenant, &key).await;
    let id = reservation_id(&first);
    assert_eq!(app.commit(&tenant, id, 5, &new_key()).await.status, StatusCode::OK);

    // The same key replays the first answer (a hold), but that hold is spent:
    // committing it again is refused, and nothing more is charged.
    let replay = reserve_item(&app, &tenant, &key).await;
    assert!(replay.replayed);
    assert_eq!(reservation_id(&replay), id);
    let again = app.commit(&tenant, id, 5, &new_key()).await;
    assert_eq!(again.status, StatusCode::CONFLICT, "{}", again.body);
    assert_eq!(app.balances(&tenant).await, (95, 0));
}

#[tokio::test]
async fn control_characters_are_refused_not_a_server_error() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_item_ctrl");
    app.grant(&tenant, 100).await;
    let feature = app.post("/v1/credits/reserve", &new_key(), item_body(&tenant, "email\u{0}lookup", ITEM, 5, 60)).await;
    assert_eq!((feature.status, feature.body["error"].clone()), (StatusCode::BAD_REQUEST, json!("invalid_feature")));
    let tenant_id = app.post("/v1/credits/reserve", &new_key(), item_body("cus\u{0}x", FEATURE, ITEM, 5, 60)).await;
    assert_eq!((tenant_id.status, tenant_id.body["error"].clone()), (StatusCode::BAD_REQUEST, json!("invalid_tenant_id")));
    let reason = app
        .post("/v1/credits/grant", &new_key(), json!({ "tenant_id": tenant, "amount": 1, "reason": "a\u{0}b" }))
        .await;
    assert_eq!((reason.status, reason.body["error"].clone()), (StatusCode::BAD_REQUEST, json!("invalid_reason")));
    assert_eq!(app.balances(&tenant).await, (100, 0));
}

#[tokio::test]
async fn a_refused_reserve_leaves_no_item_behind() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_item_poor");
    app.grant(&tenant, 1).await;
    let key = format!("email:{}", uuid::Uuid::new_v4());
    let res = app.post("/v1/credits/reserve", &new_key(), item_body(&tenant, FEATURE, &key, 5, 60)).await;
    assert_eq!(res.status, StatusCode::PAYMENT_REQUIRED, "{}", res.body);
    let items: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM items WHERE account_id = $1 AND item_key = $2")
        .bind(&app.account_id).bind(&key).fetch_one(&app.pool).await.unwrap();
    assert_eq!(items, 0, "probing items without credits must not grow the items table");
}
