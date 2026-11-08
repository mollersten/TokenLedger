//! The four concurrency scenarios from plan.md, run against a real database.

mod common;

use std::{collections::HashSet, sync::Arc, time::Duration};

use axum::http::StatusCode;
use backend::reaper;
use common::{TestApp, new_key, post, reservation_id, unique_tenant};
use serde_json::json;
use tokio::sync::Barrier;

/// Test 1: 25 simultaneous reserves of 5 credits against a 10-credit wallet.
/// Row-level locking must let exactly two through.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_1_two_button_mash_race_condition() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_race");
    app.grant(&tenant, 10).await;

    const TASKS: usize = 25;
    let barrier = Arc::new(Barrier::new(TASKS));
    let mut handles = Vec::with_capacity(TASKS);
    for i in 0..TASKS {
        let router = app.router.clone();
        let barrier = barrier.clone();
        let tenant = tenant.clone();
        handles.push(tokio::spawn(async move {
            let body = json!({
                "tenant_id": tenant,
                "feature": "ai_enrichment",
                "estimate_credits": 5,
                "ttl_seconds": 300,
                "metadata": { "task": i },
            });
            let key = format!("mash-{i}-{}", new_key());
            barrier.wait().await;
            post(router, "/v1/credits/reserve", &key, body).await
        }));
    }

    let mut ok = 0;
    let mut payment_required = 0;
    for handle in handles {
        let res = handle.await.expect("task panicked");
        match res.status {
            StatusCode::OK => {
                assert_eq!(res.body["allowed"], true);
                ok += 1;
            }
            StatusCode::PAYMENT_REQUIRED => {
                assert_eq!(res.body["allowed"], false);
                assert_eq!(res.body["error"], "insufficient_credits");
                payment_required += 1;
            }
            other => panic!("unexpected status {other}: {}", res.body),
        }
    }

    assert_eq!(ok, 2, "exactly 2 reservations should succeed");
    assert_eq!(payment_required, 23, "exactly 23 reservations should be rejected");
    assert_eq!(app.balances(&tenant).await, (0, 10), "final wallet must be available=0, reserved=10");
    assert_eq!(app.count_reservations(&tenant).await, 2);
    assert_eq!(app.count_ledger(&tenant, "RESERVE").await, 2);
    app.assert_ledger_reconciles(&tenant).await;
}

/// Test 2: 10 simultaneous reserves with the *same* Idempotency-Key.
/// One executes; the other nine replay the identical response.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_2_glitchy_wifi_idempotency_replay() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_wifi");
    app.grant(&tenant, 100).await;

    const TASKS: usize = 10;
    let key = new_key();
    let body = json!({
        "tenant_id": tenant,
        "feature": "ai_enrichment",
        "estimate_credits": 10,
        "ttl_seconds": 300,
        "metadata": { "attempt": "same" },
    });
    let barrier = Arc::new(Barrier::new(TASKS));
    let mut handles = Vec::with_capacity(TASKS);
    for _ in 0..TASKS {
        let (router, barrier, key, body) = (app.router.clone(), barrier.clone(), key.clone(), body.clone());
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            post(router, "/v1/credits/reserve", &key, body).await
        }));
    }

    let mut responses = Vec::with_capacity(TASKS);
    for handle in handles {
        responses.push(handle.await.expect("task panicked"));
    }

    for res in &responses {
        assert_eq!(res.status, StatusCode::OK, "every replay must return the original 200: {}", res.body);
        assert_eq!(res.body, responses[0].body, "every replay must return the identical payload");
    }
    let ids: HashSet<_> = responses.iter().map(reservation_id).collect();
    assert_eq!(ids.len(), 1, "all responses must reference one reservation");
    assert_eq!(responses.iter().filter(|r| r.replayed).count(), TASKS - 1, "9 responses are replays");

    assert_eq!(app.count_reservations(&tenant).await, 1, "only one reservation may be created");
    assert_eq!(app.balances(&tenant).await, (90, 10), "credits deducted exactly once");
    assert_eq!(app.count_ledger(&tenant, "RESERVE").await, 1);

    // Reusing the key with a different body, or on another endpoint, is rejected.
    let mut changed = body.clone();
    changed["estimate_credits"] = json!(11);
    let res = app.post("/v1/credits/reserve", &key, changed).await;
    assert_eq!(res.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(res.body["error"], "idempotency_key_mismatch");

    let res = app.commit(&tenant, reservation_id(&responses[0]), 5, &key).await;
    assert_eq!(res.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(res.body["error"], "idempotency_key_reused");
    assert_eq!(app.balances(&tenant).await, (90, 10), "rejected reuse must not change state");
    app.assert_ledger_reconciles(&tenant).await;
}

/// Test 3: reserve 10, commit 6 → 4 refunded to available, hold fully cleared.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_3_partial_commit_and_refund_math() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_refund");
    app.grant(&tenant, 100).await;

    let reserved = app.reserve(&tenant, 10, 300).await;
    assert_eq!(reserved.status, StatusCode::OK, "{}", reserved.body);
    assert_eq!(reserved.body["remaining_available"], 90);
    assert_eq!(reserved.body["reserved_balance"], 10);
    let id = reservation_id(&reserved);
    let (available_before, reserved_before) = app.balances(&tenant).await;

    let key = new_key();
    let committed = app.commit(&tenant, id, 6, &key).await;
    assert_eq!(committed.status, StatusCode::OK, "{}", committed.body);
    assert_eq!(committed.body["committed_amount"], 6);
    assert_eq!(committed.body["refunded_credits"], 4);
    assert_eq!(committed.body["overage_credits"], 0);

    let (available_after, reserved_after) = app.balances(&tenant).await;
    assert_eq!(available_after - available_before, 4, "available must increase by the unused 4");
    assert_eq!(reserved_before - reserved_after, 10, "reserved must drop by the full 10");
    assert_eq!((available_after, reserved_after), (94, 0));

    assert_eq!(app.reservation(id).await, ("COMMITTED".to_owned(), Some(6)));
    assert_eq!(
        app.ledger_for(id).await,
        vec![
            ("RESERVE".to_owned(), -10, 10),
            ("COMMIT".to_owned(), 0, -6),
            ("REFUND_UNUSED".to_owned(), 4, -4),
        ]
    );

    // Retrying the same commit replays; a second, different commit conflicts.
    let replay = app.commit(&tenant, id, 6, &key).await;
    assert!(replay.replayed);
    assert_eq!((replay.status, &replay.body), (committed.status, &committed.body));

    let again = app.commit(&tenant, id, 6, &new_key()).await;
    assert_eq!(again.status, StatusCode::CONFLICT);
    assert_eq!(again.body["status"], "COMMITTED");
    let release = app.release(&tenant, id, &new_key()).await;
    assert_eq!(release.status, StatusCode::CONFLICT);

    assert_eq!(app.balances(&tenant).await, (94, 0));
    app.assert_ledger_reconciles(&tenant).await;
}

/// Test 4: an abandoned reservation (ttl 1s) is expired by the background
/// reaper and its credits are returned without any client call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_4_ttl_reaper_self_healing() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_crash");
    app.grant(&tenant, 100).await;

    let reaper_task = reaper::spawn(app.pool.clone(), reaper::DEFAULT_INTERVAL);

    let reserved = app.reserve(&tenant, 10, 1).await;
    assert_eq!(reserved.status, StatusCode::OK, "{}", reserved.body);
    let id = reservation_id(&reserved);
    assert_eq!(app.balances(&tenant).await, (90, 10));

    // The client "crashes": no commit, no release.
    tokio::time::sleep(Duration::from_millis(2500)).await;

    assert_eq!(app.reservation(id).await.0, "EXPIRED", "reaper must expire the reservation");
    let wallet = app.get(&format!("/v1/tenants/{tenant}/wallet")).await;
    assert_eq!(wallet.status, StatusCode::OK);
    assert_eq!(wallet.body["available_credits"], 100, "all 10 credits restored");
    assert_eq!(wallet.body["reserved_credits"], 0);
    let latest = &wallet.body["recent_ledger"][0];
    assert_eq!(latest["entry_type"], "EXPIRE");
    assert_eq!(latest["delta_available"], 10);
    assert_eq!(latest["delta_reserved"], -10);
    assert_eq!(latest["metadata"]["source"], "reaper");

    // A late commit from the "recovered" client is refused.
    let late = app.commit(&tenant, id, 10, &new_key()).await;
    assert_eq!(late.status, StatusCode::CONFLICT);
    assert_eq!(late.body["status"], "EXPIRED");

    reaper_task.abort();
    assert_eq!(app.count_ledger(&tenant, "EXPIRE").await, 1);
    app.assert_ledger_reconciles(&tenant).await;
}
