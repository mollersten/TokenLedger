//! Edge cases beyond the four plan.md scenarios: release, overage, lazy
//! expiry, validation, ledger immutability and reaper lock behaviour.

mod common;

use std::{collections::HashSet, sync::Arc, time::Duration};

use axum::http::{Method, StatusCode};
use backend::reaper;
use common::{TestApp, new_key, post, reservation_id, send, unique_tenant};
use serde_json::json;
use tokio::sync::Barrier;

#[tokio::test]
async fn release_returns_the_full_hold() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_release");
    app.grant(&tenant, 50).await;
    let id = reservation_id(&app.reserve(&tenant, 20, 300).await);
    assert_eq!(app.balances(&tenant).await, (30, 20));

    let res = app.release(&tenant, id, &new_key()).await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.body);
    assert_eq!(res.body["released_credits"], 20);
    assert_eq!(app.balances(&tenant).await, (50, 0));
    assert_eq!(app.reservation(id).await.0, "RELEASED");
    assert_eq!(app.ledger_for(id).await.last().unwrap(), &("RELEASE".to_owned(), 20, -20));

    let again = app.release(&tenant, id, &new_key()).await;
    assert_eq!(again.status, StatusCode::CONFLICT);
    assert_eq!(again.body["status"], "RELEASED");
    let commit = app.commit(&tenant, id, 5, &new_key()).await;
    assert_eq!(commit.status, StatusCode::CONFLICT);
    app.assert_ledger_reconciles(&tenant).await;
}

#[tokio::test]
async fn commit_overage_deducts_from_available() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_overage");
    app.grant(&tenant, 20).await;
    let id = reservation_id(&app.reserve(&tenant, 10, 300).await);

    let res = app.commit(&tenant, id, 15, &new_key()).await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.body);
    assert_eq!(res.body["overage_credits"], 5);
    assert_eq!(res.body["refunded_credits"], 0);
    assert_eq!(app.balances(&tenant).await, (5, 0));
    assert_eq!(
        app.ledger_for(id).await,
        vec![
            ("RESERVE".to_owned(), -10, 10),
            ("COMMIT".to_owned(), 0, -10),
            ("OVERAGE_DEDUCT".to_owned(), -5, 0),
        ]
    );
    app.assert_ledger_reconciles(&tenant).await;
}

#[tokio::test]
async fn commit_overage_beyond_balance_is_rejected_without_side_effects() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_overage_402");
    app.grant(&tenant, 10).await;
    let id = reservation_id(&app.reserve(&tenant, 10, 300).await);

    let res = app.commit(&tenant, id, 15, &new_key()).await;
    assert_eq!(res.status, StatusCode::PAYMENT_REQUIRED, "{}", res.body);
    assert_eq!(res.body["error"], "insufficient_credits");
    assert_eq!(res.body["overage_credits"], 5);
    assert_eq!(app.balances(&tenant).await, (0, 10));
    assert_eq!(app.reservation(id).await, ("PENDING".to_owned(), None));

    // The hold is still usable for an in-budget commit.
    let ok = app.commit(&tenant, id, 10, &new_key()).await;
    assert_eq!(ok.status, StatusCode::OK);
    assert_eq!(app.balances(&tenant).await, (0, 0));
    app.assert_ledger_reconciles(&tenant).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_commits_on_one_reservation_settle_once() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_double_commit");
    app.grant(&tenant, 100).await;
    let id = reservation_id(&app.reserve(&tenant, 10, 300).await);

    const TASKS: usize = 8;
    let barrier = Arc::new(Barrier::new(TASKS));
    let mut handles = Vec::new();
    for _ in 0..TASKS {
        let (router, barrier, tenant) = (app.router.clone(), barrier.clone(), tenant.clone());
        handles.push(tokio::spawn(async move {
            let body = json!({ "tenant_id": tenant, "reservation_id": id, "actual_credits_used": 3 });
            barrier.wait().await;
            post(router, "/v1/credits/commit", &new_key(), body).await
        }));
    }
    let mut statuses = Vec::new();
    for h in handles {
        statuses.push(h.await.unwrap().status);
    }
    assert_eq!(statuses.iter().filter(|s| **s == StatusCode::OK).count(), 1);
    assert_eq!(statuses.iter().filter(|s| **s == StatusCode::CONFLICT).count(), TASKS - 1);
    assert_eq!(app.balances(&tenant).await, (97, 0));
    assert_eq!(app.count_ledger(&tenant, "COMMIT").await, 1);
    app.assert_ledger_reconciles(&tenant).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_first_grants_create_one_wallet() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_new");
    const TASKS: usize = 10;
    let barrier = Arc::new(Barrier::new(TASKS));
    let mut handles = Vec::new();
    for _ in 0..TASKS {
        let (router, barrier, tenant) = (app.router.clone(), barrier.clone(), tenant.clone());
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            post(router, "/v1/credits/grant", &new_key(), json!({ "tenant_id": tenant, "amount": 7 })).await
        }));
    }
    for h in handles {
        assert_eq!(h.await.unwrap().status, StatusCode::OK);
    }
    assert_eq!(app.balances(&tenant).await, (70, 0));
    assert_eq!(app.count_ledger(&tenant, "GRANT").await, 10);
    app.assert_ledger_reconciles(&tenant).await;
}

/// Both requests pass the lock-free idempotency fast path, then queue on the
/// wallet lock. The second must be stopped by the idempotency gate inside the
/// write statement and replay the first one's response.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_queued_behind_lock_replays_via_gate() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_gate");
    app.grant(&tenant, 100).await;

    let mut holder = app.pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM wallets WHERE tenant_id = $1 FOR UPDATE")
        .bind(&tenant)
        .execute(&mut *holder)
        .await
        .unwrap();

    let key = new_key();
    let body = json!({ "tenant_id": tenant, "feature": "gate", "estimate_credits": 10, "ttl_seconds": 300 });
    let spawn_reserve = || {
        let (router, key, body) = (app.router.clone(), key.clone(), body.clone());
        tokio::spawn(async move { post(router, "/v1/credits/reserve", &key, body).await })
    };
    let (first, second) = (spawn_reserve(), spawn_reserve());
    tokio::time::sleep(Duration::from_millis(300)).await;
    holder.rollback().await.unwrap();

    let (a, b) = (first.await.unwrap(), second.await.unwrap());
    assert_eq!((a.status, b.status), (StatusCode::OK, StatusCode::OK));
    assert_eq!(a.body, b.body);
    assert_eq!([a.replayed, b.replayed].iter().filter(|r| **r).count(), 1, "exactly one replay");
    assert_eq!(app.count_reservations(&tenant).await, 1);
    assert_eq!(app.balances(&tenant).await, (90, 10));
    app.assert_ledger_reconciles(&tenant).await;
}

#[tokio::test]
async fn overdue_reservation_is_expired_lazily_on_commit() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_lazy");
    app.grant(&tenant, 40).await;
    let id = reservation_id(&app.reserve(&tenant, 10, 1).await);
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // No reaper running: the commit itself must notice the TTL has passed.
    let res = app.commit(&tenant, id, 10, &new_key()).await;
    assert_eq!(res.status, StatusCode::CONFLICT, "{}", res.body);
    assert_eq!(res.body["status"], "EXPIRED");
    assert_eq!(app.balances(&tenant).await, (40, 0));
    assert_eq!(app.reservation(id).await.0, "EXPIRED");
    assert_eq!(app.count_ledger(&tenant, "EXPIRE").await, 1);
    app.assert_ledger_reconciles(&tenant).await;
}

#[tokio::test]
async fn reaper_skips_busy_wallets_instead_of_waiting() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_busy");
    app.grant(&tenant, 30).await;
    let id = reservation_id(&app.reserve(&tenant, 10, 1).await);
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // Simulate an in-flight request holding the wallet lock.
    let mut holder = app.pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM wallets WHERE tenant_id = $1 FOR UPDATE")
        .bind(&tenant)
        .execute(&mut *holder)
        .await
        .unwrap();

    let pass = tokio::time::timeout(Duration::from_secs(5), reaper::run_once(&app.pool, reaper::BATCH_SIZE))
        .await
        .expect("reaper must not block on a locked wallet")
        .unwrap();
    assert!(pass.skipped >= 1, "busy wallet should be skipped: {pass:?}");
    assert_eq!(app.reservation(id).await.0, "PENDING");

    holder.rollback().await.unwrap();
    for _ in 0..20 {
        let pass = reaper::run_once(&app.pool, reaper::BATCH_SIZE).await.unwrap();
        if app.reservation(id).await.0 == "EXPIRED" || pass.selected == 0 {
            break;
        }
    }
    assert_eq!(app.reservation(id).await.0, "EXPIRED");
    assert_eq!(app.balances(&tenant).await, (30, 0));
    app.assert_ledger_reconciles(&tenant).await;
}

#[tokio::test]
async fn request_validation_and_not_found() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_validation");

    let missing_key = send(
        app.router.clone(),
        Method::POST,
        "/v1/credits/grant",
        None,
        Some(json!({ "tenant_id": tenant, "amount": 5 })),
    )
    .await;
    assert_eq!(missing_key.status, StatusCode::BAD_REQUEST);
    assert_eq!(missing_key.body["error"], "missing_idempotency_key");

    let res = app.reserve(&tenant, 5, 60).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
    assert_eq!(res.body["error"], "wallet_not_found");

    let res = app.get(&format!("/v1/tenants/{tenant}/wallet")).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);

    app.grant(&tenant, 5).await;
    for (body, code) in [
        (json!({ "tenant_id": tenant, "feature": "x", "estimate_credits": 0 }), "invalid_estimate_credits"),
        (json!({ "tenant_id": tenant, "feature": "x", "estimate_credits": 1, "ttl_seconds": 0 }), "invalid_ttl_seconds"),
        (json!({ "tenant_id": tenant, "feature": "", "estimate_credits": 1 }), "invalid_feature"),
        (json!({ "tenant_id": "", "feature": "x", "estimate_credits": 1 }), "invalid_tenant_id"),
        (json!({ "tenant_id": tenant, "estimate_credits": 1 }), "invalid_request"),
    ] {
        let res = app.post("/v1/credits/reserve", &new_key(), body).await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST, "{}", res.body);
        assert_eq!(res.body["error"], code);
    }

    let res = app.grant_raw(&tenant, -5).await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);

    let unknown = app.commit(&tenant, uuid::Uuid::new_v4(), 1, &new_key()).await;
    assert_eq!(unknown.status, StatusCode::NOT_FOUND);
    assert_eq!(unknown.body["error"], "reservation_not_found");

    // Another tenant's reservation is invisible.
    let other = unique_tenant("cus_other");
    app.grant(&other, 10).await;
    let theirs = reservation_id(&app.reserve(&other, 5, 60).await);
    let res = app.release(&tenant, theirs, &new_key()).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
    assert_eq!(app.balances(&other).await, (5, 5));
}

#[tokio::test]
async fn ledger_entries_are_append_only() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_audit");
    app.grant(&tenant, 5).await;

    let update = sqlx::query("UPDATE ledger_entries SET delta_available = 999 WHERE tenant_id = $1")
        .bind(&tenant)
        .execute(&app.pool)
        .await;
    assert!(update.is_err(), "UPDATE on ledger_entries must be rejected");
    let delete = sqlx::query("DELETE FROM ledger_entries WHERE tenant_id = $1")
        .bind(&tenant)
        .execute(&app.pool)
        .await;
    assert!(delete.is_err(), "DELETE on ledger_entries must be rejected");
    assert_eq!(app.count_ledger(&tenant, "GRANT").await, 1);
}

#[tokio::test]
async fn wallet_endpoint_returns_recent_ledger_newest_first() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_history");
    for _ in 0..25 {
        app.grant(&tenant, 1).await;
    }
    let id = reservation_id(&app.reserve(&tenant, 5, 60).await);
    app.commit(&tenant, id, 2, &new_key()).await;

    let res = app.get(&format!("/v1/tenants/{tenant}/wallet")).await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(res.body["available_credits"], 23);
    assert_eq!(res.body["reserved_credits"], 0);
    let entries = res.body["recent_ledger"].as_array().unwrap();
    assert_eq!(entries.len(), 20);
    let types: Vec<_> = entries.iter().take(3).map(|e| e["entry_type"].as_str().unwrap()).collect();
    assert_eq!(types, ["REFUND_UNUSED", "COMMIT", "RESERVE"]);
    let ids: HashSet<_> = entries.iter().map(|e| e["id"].as_str().unwrap()).collect();
    assert_eq!(ids.len(), 20);
}
