//! `POST /v1/import`: moving customers over from another billing system,
//! safely rerunnable, and not usable as a bulk grant around the limits.

mod common;

use axum::http::StatusCode;
use backend::accounts::NewAccount;
use chrono::{Duration, Utc};
use common::{TestApp, TestResponse, new_key, post, reservation_id, unique_tenant};
use serde_json::{Value, json};

async fn import(app: &TestApp, key: &str, tenants: Value) -> TestResponse {
    app.post("/v1/import", key, json!({ "tenants": tenants })).await
}

fn result<'a>(res: &'a TestResponse, tenant: &str) -> &'a Value {
    res.body["results"]
        .as_array()
        .and_then(|r| r.iter().find(|t| t["tenant_id"] == tenant))
        .unwrap_or_else(|| panic!("no result for {tenant} in {}", res.body))
}

fn reserve_item(tenant: &str, item_key: &str, feature: &str) -> Value {
    json!({ "tenant_id": tenant, "feature": feature, "estimate_credits": 1, "item_key": item_key })
}

#[tokio::test]
async fn imports_balances_with_expiry_and_purchases_chargeonce_honours() {
    let app = TestApp::spawn().await;
    let (a, b) = (unique_tenant("cus_imp_a"), unique_tenant("cus_imp_b"));
    let expires = (Utc::now() + Duration::days(20)).to_rfc3339();
    let res = import(
        &app,
        "migrate-from-old",
        json!([
            {
                "tenant_id": a,
                "credits": [{ "amount": 500 }, { "amount": 100, "expires_at": expires, "reason": "november_allowance" }],
                "purchases": [
                    { "item_key": "email:1", "feature": "email_lookup", "credits": 1, "purchased_at": "2026-10-01T12:00:00Z" },
                    { "item_key": "email:2", "feature": "email_lookup", "credits": 1 }
                ]
            },
            { "tenant_id": b, "purchases": [{ "item_key": "email:1", "feature": "email_lookup", "credits": 1 }] }
        ]),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.body);
    assert_eq!((res.body["imported"].clone(), res.body["failed"].clone()), (json!(2), json!(0)));
    let ra = result(&res, &a);
    assert_eq!(ra["status"], "imported");
    assert_eq!(ra["granted_credits"], 600);
    assert_eq!(ra["purchases_imported"], 2);
    assert_eq!(ra["available_credits"], 600);
    assert_eq!(ra["expiring_credits"], 100);
    assert_eq!(app.wallet_state(&a).await, (600, 0, 100));
    app.assert_ledger_reconciles(&a).await;
    app.assert_buckets_reconcile(&a).await;

    // An imported purchase counts as paid; an item not imported is charged.
    let paid = app.post("/v1/credits/reserve", &new_key(), reserve_item(&a, "email:1", "email_lookup")).await;
    assert_eq!(paid.body["already_charged"], true, "{}", paid.body);
    assert_eq!(paid.body["charged_at"], "2026-10-01T12:00:00Z");
    let fresh = app.post("/v1/credits/reserve", &new_key(), reserve_item(&a, "email:3", "email_lookup")).await;
    assert_eq!(fresh.body["already_charged"], false, "{}", fresh.body);
    // A purchases-only tenant gets a wallet with no credits.
    assert_eq!(app.wallet_state(&b).await, (0, 0, 0));
    let b_paid = app.post("/v1/credits/reserve", &new_key(), reserve_item(&b, "email:1", "email_lookup")).await;
    assert_eq!(b_paid.body["already_charged"], true, "{}", b_paid.body);
}

#[tokio::test]
async fn rerunning_an_import_replays_instead_of_importing_twice() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_imp_rerun");
    let tenants = json!([{ "tenant_id": tenant, "credits": [{ "amount": 250 }],
                           "purchases": [{ "item_key": "email:9", "feature": "email_lookup", "credits": 2 }] }]);
    let first = import(&app, "migration-1", tenants.clone()).await;
    assert_eq!(result(&first, &tenant)["status"], "imported", "{}", first.body);
    let again = import(&app, "migration-1", tenants).await;
    assert_eq!(again.status, StatusCode::OK, "{}", again.body);
    assert_eq!(again.body["replayed"], 1);
    assert_eq!(result(&again, &tenant)["status"], "replayed");
    assert_eq!(app.balances(&tenant).await, (250, 0));
    // Imported purchases are owned items, not charge records.
    assert_eq!(app.count_reservations(&tenant).await, 0);
    assert_eq!(app.count_owned_items(&tenant).await, 1);

    // Same key, changed data (the old system moved on): refused, not re-imported.
    let changed = import(&app, "migration-1", json!([{ "tenant_id": tenant, "credits": [{ "amount": 300 }] }])).await;
    let r = result(&changed, &tenant);
    assert_eq!((r["status"].clone(), r["error"].clone()), (json!("failed"), json!("idempotency_key_mismatch")));
    assert_eq!(app.balances(&tenant).await, (250, 0));
}

#[tokio::test]
async fn a_balance_can_only_be_imported_once() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_imp_once");
    import(&app, &new_key(), json!([{ "tenant_id": tenant, "credits": [{ "amount": 100 }] }])).await;

    // Another import (new key) cannot add credits again: that would be a bulk
    // grant around the per-request limits.
    let res = import(&app, &new_key(), json!([{ "tenant_id": tenant, "credits": [{ "amount": 100 }] }])).await;
    let r = result(&res, &tenant);
    assert_eq!((r["status"].clone(), r["error"].clone()), (json!("failed"), json!("credits_already_imported")));
    assert_eq!(app.balances(&tenant).await, (100, 0));

    // Purchases can still be imported later; ones already there are skipped.
    let item = json!({ "item_key": "email:1", "feature": "email_lookup", "credits": 1 });
    let p1 = import(&app, &new_key(), json!([{ "tenant_id": tenant, "purchases": [item] }])).await;
    assert_eq!(result(&p1, &tenant)["purchases_imported"], 1, "{}", p1.body);
    let item2 = json!({ "item_key": "email:2", "feature": "email_lookup", "credits": 1 });
    let p2 = import(&app, &new_key(), json!([{ "tenant_id": tenant, "purchases": [item, item2] }])).await;
    let r2 = result(&p2, &tenant);
    assert_eq!((r2["purchases_imported"].clone(), r2["purchases_skipped"].clone()), (json!(1), json!(1)));
    assert_eq!(r2["skipped_item_keys"], json!(["email:1"]));
    assert_eq!(app.balances(&tenant).await, (100, 0));
}

#[tokio::test]
async fn one_failing_tenant_does_not_undo_the_others() {
    let app = TestApp::spawn().await;
    let (good, bad) = (unique_tenant("cus_imp_good"), unique_tenant("cus_imp_bad"));
    let past = (Utc::now() - Duration::days(1)).to_rfc3339();
    let res = import(
        &app,
        "migration-partial",
        json!([
            { "tenant_id": good, "credits": [{ "amount": 40 }] },
            // Expired credits are refused, and the tenant's other grants roll back with them.
            { "tenant_id": bad, "credits": [{ "amount": 70 }, { "amount": 30, "expires_at": past }] }
        ]),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.body);
    assert_eq!((res.body["imported"].clone(), res.body["failed"].clone()), (json!(1), json!(1)));
    assert_eq!(result(&res, &bad)["error"], "invalid_expires_at");
    assert_eq!(app.balances(&good).await, (40, 0));
    let wallet: Option<i64> = sqlx::query_scalar("SELECT available_credits FROM wallets WHERE account_id = $1 AND tenant_id = $2")
        .bind(&app.account_id)
        .bind(&bad)
        .fetch_optional(&app.pool)
        .await
        .unwrap();
    assert_eq!(wallet, None, "a failed tenant leaves nothing behind");

    // Fixed and retried under the same key: the good one replays, the bad one imports.
    let retry = import(
        &app,
        "migration-partial",
        json!([{ "tenant_id": good, "credits": [{ "amount": 40 }] }, { "tenant_id": bad, "credits": [{ "amount": 70 }] }]),
    )
    .await;
    assert_eq!((retry.body["replayed"].clone(), retry.body["imported"].clone()), (json!(1), json!(1)), "{}", retry.body);
    assert_eq!(app.balances(&bad).await, (70, 0));
}

#[tokio::test]
async fn an_item_already_paid_is_not_overwritten_by_an_import() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_imp_live");
    app.grant(&tenant, 10).await;
    let live = app.post("/v1/credits/reserve", &new_key(), reserve_item(&tenant, "email:1", "email_lookup")).await;
    assert_eq!(app.commit(&tenant, reservation_id(&live), 1, &new_key()).await.status, StatusCode::OK);

    let res = import(&app, &new_key(), json!([{ "tenant_id": tenant, "purchases": [
        { "item_key": "email:1", "feature": "email_lookup", "credits": 1 }] }])).await;
    assert_eq!(result(&res, &tenant)["purchases_skipped"], 1, "{}", res.body);
    // The live charge is the only record, and nothing was charged twice.
    assert_eq!(app.count_reservations(&tenant).await, 1);
    assert_eq!(app.balances(&tenant).await, (9, 0));
}

#[tokio::test]
async fn importing_respects_the_tenant_limit() {
    let mut account = NewAccount::unlimited("import tenant limit");
    account.max_tenants = Some(1);
    let app = TestApp::spawn_with(account).await;
    let (t1, t2) = (unique_tenant("cus_imp_l1"), unique_tenant("cus_imp_l2"));
    let res = import(&app, &new_key(), json!([
        { "tenant_id": t1, "credits": [{ "amount": 5 }] },
        { "tenant_id": t2, "credits": [{ "amount": 5 }] }
    ]))
    .await;
    assert_eq!(result(&res, &t1)["status"], "imported", "{}", res.body);
    assert_eq!(result(&res, &t2)["error"], "tenant_limit_reached");
}

#[tokio::test]
async fn imports_do_not_count_toward_any_limit() {
    let mut account = NewAccount::free("import limits");
    account.hourly_request_limit = Some(2);
    account.grace_hourly_request_limit = None;
    account.monthly_request_limit = Some(2);
    account.rate_limit_per_sec = Some(1);
    let app = TestApp::spawn_with(account).await;

    // Five imports in a row, past every limit the account has.
    for _ in 0..5 {
        let tenants: Vec<Value> = (0..100)
            .map(|i| json!({ "tenant_id": format!("{}_{i}", unique_tenant("cus_imp_many")), "credits": [{ "amount": 10 }] }))
            .collect();
        let res = import(&app, &new_key(), json!(tenants)).await;
        assert_eq!(res.status, StatusCode::OK, "{}", res.body);
        assert_eq!(res.body["imported"], 100);
        assert!(!res.headers.contains_key("x-ratelimit-remaining"), "imports are not counted");
    }

    // The customers' own actions still count from the first one.
    let tenant = unique_tenant("cus_imp_after");
    let first = app.grant_raw(&tenant, 5).await;
    assert_eq!(first.status, StatusCode::OK, "{}", first.body);
    assert_eq!(first.headers["x-ratelimit-remaining"], "1");
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    assert_eq!(app.grant_raw(&tenant, 5).await.status, StatusCode::OK);
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let over = app.grant_raw(&tenant, 5).await;
    assert_eq!(over.status, StatusCode::TOO_MANY_REQUESTS, "{}", over.body);

    // And an account at its limit can still import.
    let res = import(&app, &new_key(), json!([{ "tenant_id": unique_tenant("cus_imp_late"), "credits": [{ "amount": 1 }] }])).await;
    assert_eq!(res.body["imported"], 1, "{}", res.body);
}

async fn wallet_count(app: &TestApp) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM wallets WHERE account_id = $1")
        .bind(&app.account_id)
        .fetch_one(&app.pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn a_malformed_request_imports_nothing() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_imp_bad_req");
    let too_many: Vec<Value> = (0..101).map(|i| json!({ "tenant_id": format!("t{i}"), "credits": [{ "amount": 1 }] })).collect();
    let too_many_purchases: Vec<Value> =
        (0..5_001).map(|i| json!({ "item_key": format!("email:{i}"), "feature": "f", "credits": 1 })).collect();
    let cases = [
        json!({ "tenants": too_many }),
        json!({ "tenants": [] }),
        json!({ "tenants": [{ "tenant_id": tenant, "purchases": too_many_purchases }] }),
        json!({ "tenants": [{ "tenant_id": tenant, "credits": [{ "amount": 1 }] }], "extra": true }),
        json!([{ "tenant_id": tenant, "credits": [{ "amount": 1 }] }]),
    ];
    for body in cases {
        let res = post(&app.api, "/v1/import", &new_key(), body.clone()).await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST, "{}: {}", body.to_string().chars().take(120).collect::<String>(), res.body);
    }
    assert_eq!(wallet_count(&app).await, 0);
}

#[tokio::test]
async fn an_invalid_tenant_is_reported_and_the_others_are_imported() {
    let app = TestApp::spawn().await;
    let good = unique_tenant("cus_imp_ok");
    let item = json!({ "item_key": "email:1", "feature": "email_lookup", "credits": 1 });
    let future = (Utc::now() + Duration::days(1)).to_rfc3339();
    let bad: Vec<(Value, &str)> = vec![
        (json!({ "tenant_id": "b_nothing" }), "invalid_import"),
        (json!({ "tenant_id": "b_zero", "credits": [{ "amount": 0 }] }), "invalid_import"),
        (json!({ "tenant_id": "b_typo", "purchase": [item] }), "invalid_request"),
        (json!({ "tenant_id": "b_dup_item", "purchases": [item, item] }), "invalid_import"),
        (json!({ "tenant_id": "b_free_item", "purchases": [{ "item_key": "x", "feature": "f", "credits": 0 }] }), "invalid_import"),
        (json!({ "tenant_id": "b_date", "credits": [{ "amount": 1, "expires_at": "next tuesday" }] }), "invalid_request"),
        (json!({ "tenant_id": "b_nul", "credits": [{ "amount": 1, "reason": "a\u{0}b" }] }), "invalid_reason"),
        (json!({ "tenant_id": "b_ctrl", "purchases": [{ "item_key": "x", "feature": "f\u{0}", "credits": 1 }] }), "invalid_feature"),
        (json!({ "tenant_id": "b_future", "purchases": [{ "item_key": "x", "feature": "f", "credits": 1, "purchased_at": future }] }), "invalid_purchased_at"),
        (json!({ "tenant_id": good, "credits": [{ "amount": 1 }] }), "invalid_import"), // second time: duplicate
    ];
    let mut tenants = vec![json!({ "tenant_id": good, "credits": [{ "amount": 30 }] })];
    tenants.extend(bad.iter().map(|(t, _)| t.clone()));
    let res = import(&app, &new_key(), json!(tenants)).await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.body);
    assert_eq!((res.body["imported"].clone(), res.body["failed"].clone()), (json!(1), json!(bad.len())), "{}", res.body);
    let results = res.body["results"].as_array().unwrap();
    for ((_, code), r) in bad.iter().zip(&results[1..]) {
        assert_eq!((r["status"].as_str(), r["error"].as_str()), (Some("failed"), Some(*code)), "{r}");
    }
    assert_eq!(app.balances(&good).await, (30, 0));
    assert_eq!(wallet_count(&app).await, 1, "failed tenants leave nothing behind");
}

#[tokio::test]
async fn an_import_larger_than_two_megabytes_is_accepted() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_imp_big");
    let long = "f".repeat(255);
    let purchases: Vec<Value> = (0..5_000)
        .map(|i| json!({ "item_key": format!("{i:0>255}"), "feature": long, "credits": 1 }))
        .collect();
    let body = json!({ "tenants": [{ "tenant_id": tenant, "purchases": purchases }] });
    assert!(body.to_string().len() > 2 * 1024 * 1024);
    let res = post(&app.api, "/v1/import", &new_key(), body).await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.body);
    assert_eq!(result(&res, &tenant)["purchases_imported"], 5_000);
}

#[tokio::test]
async fn an_item_being_charged_holds_back_the_import_until_it_settles() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_imp_pending");
    app.grant(&tenant, 10).await;
    let pending = app.post("/v1/credits/reserve", &new_key(), reserve_item(&tenant, "email:1", "email_lookup")).await;
    let id = reservation_id(&pending);
    let tenants = json!([{ "tenant_id": tenant, "purchases": [
        { "item_key": "email:1", "feature": "email_lookup", "credits": 1 },
        { "item_key": "email:2", "feature": "email_lookup", "credits": 1 }] }]);

    // Importing now could lose the old purchase if the live charge is then
    // released, so the tenant is refused for the moment.
    let key = new_key();
    let busy = import(&app, &key, tenants.clone()).await;
    let r = result(&busy, &tenant);
    assert_eq!((r["status"].clone(), r["error"].clone()), (json!("failed"), json!("item_charge_in_progress")), "{r}");
    assert_eq!(app.count_reservations(&tenant).await, 1);

    // Released (the live attempt failed): retrying the same import now records
    // the old purchase, so the customer is not charged for it again.
    assert_eq!(app.release(&tenant, id, &new_key()).await.status, StatusCode::OK);
    let retry = import(&app, &key, tenants).await;
    let r = result(&retry, &tenant);
    assert_eq!((r["status"].clone(), r["purchases_imported"].clone()), (json!("imported"), json!(2)), "{r}");
    let again = app.post("/v1/credits/reserve", &new_key(), reserve_item(&tenant, "email:1", "email_lookup")).await;
    assert_eq!(again.body["already_charged"], true, "{}", again.body);
}

fn caps(customers: i64, purchases: i64) -> backend::import::ImportCaps {
    backend::import::ImportCaps { customers_per_day: customers, purchases_per_day: purchases }
}

#[tokio::test]
async fn the_daily_cap_counts_each_customer_moved_once() {
    let app = TestApp::spawn_with_import_caps(caps(3, 1_000)).await;
    let ids: Vec<String> = (0..5).map(|i| unique_tenant(&format!("cus_cap_{i}"))).collect();
    let tenants: Vec<Value> = ids.iter().map(|t| json!({ "tenant_id": t, "credits": [{ "amount": 1 }] })).collect();
    let res = import(&app, "cap-run", json!(tenants)).await;
    assert_eq!((res.body["imported"].clone(), res.body["failed"].clone()), (json!(3), json!(2)), "{}", res.body);
    assert_eq!(result(&res, &ids[4])["error"], "import_limit_reached");

    // Rerunning replays the three, which don't count again; the cap still holds.
    let again = import(&app, "cap-run", json!(tenants)).await;
    assert_eq!((again.body["replayed"].clone(), again.body["failed"].clone()), (json!(3), json!(2)), "{}", again.body);

    // A follow-up import of purchases for a customer already moved is free.
    let follow_up = import(&app, &new_key(), json!([{ "tenant_id": ids[0], "purchases": [
        { "item_key": "email:1", "feature": "email_lookup", "credits": 1 }] }])).await;
    assert_eq!(result(&follow_up, &ids[0])["status"], "imported", "{}", follow_up.body);
    let used: (i64, i64) = sqlx::query_as("SELECT customers, purchases FROM account_import_usage WHERE account_id = $1")
        .bind(&app.account_id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(used, (3, 1));
}

#[tokio::test]
async fn the_daily_cap_on_purchases_counts_only_rows_written() {
    let app = TestApp::spawn_with_import_caps(caps(1_000, 5)).await;
    let (a, b, c) = (unique_tenant("cus_pcap_a"), unique_tenant("cus_pcap_b"), unique_tenant("cus_pcap_c"));
    let items = |n: usize| -> Vec<Value> {
        (0..n).map(|i| json!({ "item_key": format!("email:{i}"), "feature": "email_lookup", "credits": 1 })).collect()
    };
    let res = import(&app, &new_key(), json!([
        { "tenant_id": a, "purchases": items(3) },
        { "tenant_id": b, "purchases": items(3) },
        { "tenant_id": c, "purchases": items(2) }
    ]))
    .await;
    let statuses: Vec<_> = [&a, &b, &c].iter().map(|t| result(&res, t)["status"].clone()).collect();
    assert_eq!(statuses, [json!("imported"), json!("failed"), json!("imported")], "{}", res.body);
    assert_eq!(result(&res, &b)["error"], "import_limit_reached");

    // Items already paid are skipped, write nothing, and so don't count.
    let again = import(&app, &new_key(), json!([{ "tenant_id": a, "purchases": items(3) }])).await;
    assert_eq!(result(&again, &a)["purchases_skipped"], 3, "{}", again.body);
}

#[tokio::test]
async fn once_both_caps_are_used_up_imports_are_refused_up_front() {
    let app = TestApp::spawn_with_import_caps(caps(1, 1)).await;
    let t = unique_tenant("cus_cap_full");
    let res = import(&app, &new_key(), json!([{ "tenant_id": t, "credits": [{ "amount": 1 }],
        "purchases": [{ "item_key": "email:1", "feature": "f", "credits": 1 }] }])).await;
    assert_eq!(result(&res, &t)["status"], "imported", "{}", res.body);
    let refused = import(&app, &new_key(), json!([{ "tenant_id": unique_tenant("cus_cap_more"), "credits": [{ "amount": 1 }] }])).await;
    assert_eq!(refused.status, StatusCode::TOO_MANY_REQUESTS, "{}", refused.body);
    assert_eq!(refused.body["error"], "import_limit_reached");
    assert!(refused.headers.contains_key("retry-after"));
}

#[tokio::test]
async fn at_most_two_imports_run_at_once_per_account() {
    let app = TestApp::spawn().await;
    let _one = app.state.gates.imports.try_acquire(&app.account_id).unwrap();
    let _two = app.state.gates.imports.try_acquire(&app.account_id).unwrap();
    let res = import(&app, &new_key(), json!([{ "tenant_id": unique_tenant("cus_imp_busy"), "credits": [{ "amount": 1 }] }])).await;
    assert_eq!(res.status, StatusCode::CONFLICT, "{}", res.body);
    assert_eq!(res.body["error"], "import_in_progress");
}

#[tokio::test]
async fn rerunning_an_import_after_its_records_were_pruned_still_imports_nothing_twice() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_imp_pruned");
    let tenants = json!([{ "tenant_id": tenant, "credits": [{ "amount": 40 }],
                           "purchases": [{ "item_key": "email:1", "feature": "email_lookup", "credits": 1 }] }]);
    import(&app, "migration-old", tenants.clone()).await;
    // A day later the idempotency records are gone.
    sqlx::query("DELETE FROM idempotency_keys WHERE account_id = $1").bind(&app.account_id).execute(&app.pool).await.unwrap();

    let rerun = import(&app, "migration-old", tenants.clone()).await;
    let r = result(&rerun, &tenant);
    assert_eq!((r["status"].clone(), r["granted_credits"].clone(), r["purchases_skipped"].clone()),
               (json!("replayed"), json!(0), json!(1)), "{r}");
    assert_eq!(app.balances(&tenant).await, (40, 0));
    // A different import still cannot bring the balance in again.
    let other = import(&app, "migration-new", tenants).await;
    assert_eq!(result(&other, &tenant)["error"], "credits_already_imported");
}

#[tokio::test]
async fn an_imported_purchase_older_than_the_retention_is_not_owned() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_imp_old");
    sqlx::query("UPDATE accounts SET item_retention_days = 30 WHERE id = $1").bind(&app.account_id).execute(&app.pool).await.unwrap();
    let old = (Utc::now() - Duration::days(45)).to_rfc3339();
    let fresh = (Utc::now() - Duration::days(5)).to_rfc3339();
    import(&app, &new_key(), json!([{ "tenant_id": tenant, "credits": [{ "amount": 10 }], "purchases": [
        { "item_key": "email:old", "feature": "email_lookup", "credits": 1, "purchased_at": old },
        { "item_key": "email:new", "feature": "email_lookup", "credits": 1, "purchased_at": fresh }] }])).await;
    let old_item = app.post("/v1/credits/reserve", &new_key(), reserve_item(&tenant, "email:old", "email_lookup")).await;
    assert_eq!(old_item.body["already_charged"], false, "{}", old_item.body);
    let new_item = app.post("/v1/credits/reserve", &new_key(), reserve_item(&tenant, "email:new", "email_lookup")).await;
    assert_eq!(new_item.body["already_charged"], true, "{}", new_item.body);
}
