//! Expiring credits: spend order, monthly allowances that lapse, plan credits
//! next to top-ups that never expire, refunds into expired buckets, and no
//! credit ever being spent after it expires.

mod common;

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::http::StatusCode;
use backend::reaper;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use common::{TestApp, new_key, post, reservation_id, unique_tenant};
use serde_json::json;
use tokio::sync::Barrier;
use uuid::Uuid;

fn in_secs(secs: i64) -> DateTime<Utc> {
    Utc::now() + ChronoDuration::seconds(secs)
}

fn in_millis(ms: i64) -> DateTime<Utc> {
    Utc::now() + ChronoDuration::milliseconds(ms)
}

async fn sleep_until(at: DateTime<Utc>) {
    let wait = (at - Utc::now()).to_std().unwrap_or_default();
    tokio::time::sleep(wait + Duration::from_millis(50)).await;
}

fn bucket_ids(body: &serde_json::Value) -> Vec<(Uuid, i64)> {
    body["credit_buckets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| (b["id"].as_str().unwrap().parse().unwrap(), b["remaining"].as_i64().unwrap()))
        .collect()
}

#[tokio::test]
async fn buckets_are_spent_first_soonest_expiry_first_and_refunds_go_back() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_order");
    app.grant(&tenant, 100).await; // top-up: never expires
    let later = app.grant_expiring(&tenant, 30, in_secs(7200)).await;
    let sooner = app.grant_expiring(&tenant, 50, in_secs(3600)).await;
    assert_eq!(app.wallet_state(&tenant).await, (180, 0, 80));

    // 60 = all 50 of the sooner bucket + 10 of the later one; top-up untouched.
    let id = reservation_id(&app.reserve(&tenant, 60, 300).await);
    assert_eq!(app.wallet_state(&tenant).await, (120, 60, 20));
    assert_eq!(app.bucket_remaining(sooner).await, 0);
    assert_eq!(app.bucket_remaining(later).await, 20);

    // Used 40: consumed from the sooner bucket; the unused 20 goes back to
    // where it came from (10 to each bucket).
    let res = app.commit(&tenant, id, 40, &new_key()).await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.body);
    assert_eq!(res.body["refunded_credits"], 20);
    assert_eq!(res.body["expired_credits"], 0);
    assert_eq!(res.body["expiring_credits"], 40);
    assert_eq!(app.bucket_remaining(sooner).await, 10);
    assert_eq!(app.bucket_remaining(later).await, 30);
    assert_eq!(app.wallet_state(&tenant).await, (140, 0, 40));

    let wallet = app.get(&format!("/v1/tenants/{tenant}/wallet")).await;
    assert_eq!(wallet.status, StatusCode::OK);
    assert_eq!(wallet.body["available_credits"], 140);
    assert_eq!(wallet.body["expiring_credits"], 40);
    assert_eq!(wallet.body["permanent_credits"], 100);
    assert_eq!(bucket_ids(&wallet.body), vec![(sooner, 10), (later, 30)]);
    assert_eq!(wallet.body["credit_buckets"][0]["reason"], "monthly_allowance");
    assert_eq!(wallet.body["next_expiry_at"], wallet.body["credit_buckets"][0]["expires_at"]);

    app.assert_ledger_reconciles(&tenant).await;
    app.assert_buckets_reconcile(&tenant).await;
}

/// A monthly allowance next to a top-up: the unused allowance lapses at the
/// period end, the top-up stays, and the next period's grant starts fresh.
#[tokio::test]
async fn unused_allowance_expires_and_top_ups_survive() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_monthly");
    app.grant(&tenant, 100).await;
    let period_end = in_millis(1200);
    let allowance = app.grant_expiring(&tenant, 50, period_end).await;

    let id = reservation_id(&app.reserve(&tenant, 20, 300).await);
    assert_eq!(app.commit(&tenant, id, 20, &new_key()).await.status, StatusCode::OK);
    assert_eq!(app.wallet_state(&tenant).await, (130, 0, 30));

    sleep_until(period_end).await;

    // Reads leave expired credits out at once, before anything is written.
    let wallet = app.get(&format!("/v1/tenants/{tenant}/wallet")).await;
    assert_eq!(wallet.body["available_credits"], 100);
    assert_eq!(wallet.body["expiring_credits"], 0);
    assert_eq!(wallet.body["credit_buckets"], json!([]));

    // The next request on the wallet expires the bucket itself, so the 30
    // leftover allowance credits can't be spent.
    let denied = app.reserve(&tenant, 101, 300).await;
    assert_eq!(denied.status, StatusCode::PAYMENT_REQUIRED, "{}", denied.body);
    assert_eq!(denied.body["available_credits"], 100);
    assert_eq!(app.wallet_state(&tenant).await, (100, 0, 0));
    assert_eq!(app.bucket_remaining(allowance).await, 0);
    let expired: Vec<(i64, Option<Uuid>)> = sqlx::query_as(
        "SELECT delta_available, bucket_id FROM ledger_entries \
         WHERE account_id = $1 AND tenant_id = $2 AND entry_type = 'CREDIT_EXPIRE'",
    )
    .bind(&app.account_id)
    .bind(&tenant)
    .fetch_all(&app.pool)
    .await
    .unwrap();
    assert_eq!(expired, vec![(-30, Some(allowance))]);

    // Next period.
    app.grant_expiring(&tenant, 50, in_secs(3600)).await;
    assert_eq!(app.wallet_state(&tenant).await, (150, 0, 50));
    app.assert_ledger_reconciles(&tenant).await;
    app.assert_buckets_reconcile(&tenant).await;
}

#[tokio::test]
async fn reaper_expires_due_buckets() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_reap_bucket");
    app.grant(&tenant, 5).await;
    let at = in_millis(800);
    let bucket = app.grant_expiring(&tenant, 40, at).await;
    // A second, sooner bucket that is fully spent before it expires.
    let emptied = app.grant_expiring(&tenant, 10, at - ChronoDuration::milliseconds(100)).await;
    let id = reservation_id(&app.reserve(&tenant, 10, 300).await);
    assert_eq!(app.commit(&tenant, id, 10, &new_key()).await.status, StatusCode::OK);
    assert_eq!(app.bucket_remaining(emptied).await, 0);
    sleep_until(at).await;

    let settled = |id: Uuid| {
        let pool = app.pool.clone();
        async move {
            sqlx::query_scalar::<_, bool>("SELECT settled_at IS NOT NULL FROM credit_buckets WHERE id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    // Other tests' buckets may be due too, so drain until ours are done.
    for _ in 0..20 {
        reaper::run_once(&app.pool, reaper::BATCH_SIZE).await.unwrap();
        if settled(bucket).await && settled(emptied).await {
            break;
        }
    }
    assert!(settled(bucket).await && settled(emptied).await, "both due buckets settle");
    assert_eq!(app.bucket_remaining(bucket).await, 0);
    assert_eq!(app.wallet_state(&tenant).await, (5, 0, 0));
    // Only the bucket that still had credits gets a CREDIT_EXPIRE entry.
    assert_eq!(app.count_ledger(&tenant, "CREDIT_EXPIRE").await, 1);
    app.assert_ledger_reconciles(&tenant).await;
    app.assert_buckets_reconcile(&tenant).await;
}

/// Credits held when their bucket expires can still be used by that
/// request; whatever comes back unused expires on return.
#[tokio::test]
async fn unused_hold_from_an_expired_bucket_expires_on_return() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_hold_expiry");
    app.grant(&tenant, 10).await;
    let at = in_millis(1000);
    let bucket = app.grant_expiring(&tenant, 30, at).await;
    let a = reservation_id(&app.reserve(&tenant, 20, 300).await);
    let b = reservation_id(&app.reserve(&tenant, 10, 300).await);
    assert_eq!(app.wallet_state(&tenant).await, (10, 30, 0));
    sleep_until(at).await;

    let commit = app.commit(&tenant, a, 5, &new_key()).await;
    assert_eq!(commit.status, StatusCode::OK, "{}", commit.body);
    assert_eq!(commit.body["refunded_credits"], 15);
    assert_eq!(commit.body["expired_credits"], 15);
    assert_eq!(
        app.ledger_for(a).await,
        vec![
            ("RESERVE".to_owned(), -20, 20),
            ("COMMIT".to_owned(), 0, -5),
            ("REFUND_UNUSED".to_owned(), 15, -15),
            ("CREDIT_EXPIRE".to_owned(), -15, 0),
        ]
    );

    let release = app.release(&tenant, b, &new_key()).await;
    assert_eq!(release.status, StatusCode::OK, "{}", release.body);
    assert_eq!(release.body["expired_credits"], 10);

    // Only the top-up is left; the bucket was never refilled.
    assert_eq!(app.wallet_state(&tenant).await, (10, 0, 0));
    assert_eq!(app.bucket_remaining(bucket).await, 0);
    app.assert_ledger_reconciles(&tenant).await;
    app.assert_buckets_reconcile(&tenant).await;
}

#[tokio::test]
async fn hold_returned_before_expiry_goes_back_to_its_bucket() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_release_bucket");
    let bucket = app.grant_expiring(&tenant, 25, in_secs(3600)).await;
    let id = reservation_id(&app.reserve(&tenant, 25, 300).await);
    assert_eq!(app.wallet_state(&tenant).await, (0, 25, 0));

    let res = app.release(&tenant, id, &new_key()).await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.body);
    assert_eq!(res.body["expired_credits"], 0);
    assert_eq!(app.bucket_remaining(bucket).await, 25);
    assert_eq!(app.wallet_state(&tenant).await, (25, 0, 25));
    app.assert_ledger_reconciles(&tenant).await;
    app.assert_buckets_reconcile(&tenant).await;
}

#[tokio::test]
async fn overage_is_taken_from_buckets_first() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_bucket_overage");
    app.grant(&tenant, 10).await;
    let id = reservation_id(&app.reserve(&tenant, 10, 300).await);
    let bucket = app.grant_expiring(&tenant, 20, in_secs(3600)).await;

    let res = app.commit(&tenant, id, 15, &new_key()).await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.body);
    assert_eq!(res.body["overage_credits"], 5);
    assert_eq!(app.bucket_remaining(bucket).await, 15);
    assert_eq!(app.wallet_state(&tenant).await, (15, 0, 15));
    app.assert_ledger_reconciles(&tenant).await;
    app.assert_buckets_reconcile(&tenant).await;
}

#[tokio::test]
async fn expiring_grant_validation_and_replay() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_bucket_validation");

    let past = app
        .post(
            "/v1/credits/grant",
            &new_key(),
            json!({ "tenant_id": tenant, "amount": 10, "expires_at": in_secs(-60).to_rfc3339() }),
        )
        .await;
    assert_eq!(past.status, StatusCode::UNPROCESSABLE_ENTITY, "{}", past.body);
    assert_eq!(past.body["error"], "invalid_expires_at");
    // The failed grant didn't leave a wallet behind.
    assert_eq!(app.get(&format!("/v1/tenants/{tenant}/wallet")).await.status, StatusCode::NOT_FOUND);

    let garbage = app
        .post("/v1/credits/grant", &new_key(), json!({ "tenant_id": tenant, "amount": 10, "expires_at": "soon" }))
        .await;
    assert_eq!(garbage.status, StatusCode::BAD_REQUEST, "{}", garbage.body);

    // Retrying an expiring grant with the same key creates one bucket.
    let key = new_key();
    let body = json!({ "tenant_id": tenant, "amount": 10, "expires_at": in_secs(3600).to_rfc3339() });
    let first = app.post("/v1/credits/grant", &key, body.clone()).await;
    let second = app.post("/v1/credits/grant", &key, body).await;
    assert_eq!(first.status, StatusCode::OK, "{}", first.body);
    assert!(second.replayed);
    assert_eq!(first.body["bucket_id"], second.body["bucket_id"]);
    let buckets: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM credit_buckets WHERE account_id = $1 AND tenant_id = $2")
            .bind(&app.account_id)
            .bind(&tenant)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(buckets, 1);
    assert_eq!(app.wallet_state(&tenant).await, (10, 0, 10));
}

/// The race test from plan.md, on a wallet funded by a bucket plus a
/// top-up: 25 simultaneous reserves of 5 against 6 + 4 credits.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn parallel_reserves_never_overdraw_mixed_credits() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_bucket_race");
    app.grant(&tenant, 4).await;
    let bucket = app.grant_expiring(&tenant, 6, in_secs(3600)).await;

    const TASKS: usize = 25;
    let barrier = Arc::new(Barrier::new(TASKS));
    let mut handles = Vec::new();
    for i in 0..TASKS {
        let api = app.api.clone();
        let barrier = barrier.clone();
        let tenant = tenant.clone();
        handles.push(tokio::spawn(async move {
            let body = json!({ "tenant_id": tenant, "feature": "render", "estimate_credits": 5, "ttl_seconds": 300 });
            let key = format!("mixed-{i}-{}", new_key());
            barrier.wait().await;
            post(&api, "/v1/credits/reserve", &key, body).await.status
        }));
    }
    let mut ok = 0;
    for handle in handles {
        match handle.await.unwrap() {
            StatusCode::OK => ok += 1,
            StatusCode::PAYMENT_REQUIRED => {}
            other => panic!("unexpected status {other}"),
        }
    }
    assert_eq!(ok, 2);
    assert_eq!(app.wallet_state(&tenant).await, (0, 10, 0));
    assert_eq!(app.bucket_remaining(bucket).await, 0);
    app.assert_ledger_reconciles(&tenant).await;
    app.assert_buckets_reconcile(&tenant).await;
}

/// Hammer a wallet whose only credits expire mid-run, with the reaper
/// running alongside: every credit is consumed or expired exactly once, and
/// nothing is reserved from the bucket after its expiry.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn no_credit_is_spent_after_it_expires() {
    let app = TestApp::spawn().await;
    let tenant = unique_tenant("cus_expiry_race");
    let expires_at = in_millis(1000);
    let bucket = app.grant_expiring(&tenant, 1_000_000, expires_at).await;

    let stop = Arc::new(AtomicBool::new(false));
    let mut workers = Vec::new();
    for w in 0..8 {
        let api = app.api.clone();
        let tenant = tenant.clone();
        let stop = stop.clone();
        workers.push(tokio::spawn(async move {
            let (mut committed, mut denied) = (0, 0);
            while !stop.load(Ordering::Relaxed) {
                let reserve = post(
                    &api,
                    "/v1/credits/reserve",
                    &format!("w{w}-{}", new_key()),
                    json!({ "tenant_id": tenant, "feature": "f", "estimate_credits": 3, "ttl_seconds": 300 }),
                )
                .await;
                match reserve.status {
                    StatusCode::OK => {
                        let body = json!({
                            "tenant_id": tenant,
                            "reservation_id": reservation_id(&reserve),
                            "actual_credits_used": 2,
                        });
                        let commit = post(&api, "/v1/credits/commit", &new_key(), body).await;
                        assert_eq!(commit.status, StatusCode::OK, "{}", commit.body);
                        committed += 1;
                    }
                    StatusCode::PAYMENT_REQUIRED => denied += 1,
                    other => panic!("unexpected status {other}: {}", reserve.body),
                }
            }
            (committed, denied)
        }));
    }
    let reaper_pool = app.pool.clone();
    let reaper_stop = stop.clone();
    let reaper_task = tokio::spawn(async move {
        while !reaper_stop.load(Ordering::Relaxed) {
            reaper::run_once(&reaper_pool, reaper::BATCH_SIZE).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });

    sleep_until(expires_at + ChronoDuration::milliseconds(500)).await;
    stop.store(true, Ordering::Relaxed);
    let (mut committed, mut denied) = (0, 0);
    for worker in workers {
        let (c, d) = worker.await.unwrap();
        committed += c;
        denied += d;
    }
    reaper_task.await.unwrap();

    assert!(committed > 0, "some work should have run before the expiry");
    assert!(denied > 0, "requests after the expiry must be refused");
    assert_eq!(app.bucket_remaining(bucket).await, 0);
    assert_eq!(app.wallet_state(&tenant).await, (0, 0, 0));
    let late: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reservations WHERE account_id = $1 AND tenant_id = $2 AND created_at >= $3",
    )
    .bind(&app.account_id)
    .bind(&tenant)
    .bind(expires_at)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(late, 0, "no reservation may start at or after the bucket's expiry");
    let expired: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(-delta_available), 0)::bigint FROM ledger_entries \
         WHERE account_id = $1 AND tenant_id = $2 AND entry_type = 'CREDIT_EXPIRE'",
    )
    .bind(&app.account_id)
    .bind(&tenant)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(expired + 2 * committed, 1_000_000, "every credit is either used or expired");
    app.assert_ledger_reconciles(&tenant).await;
    app.assert_buckets_reconcile(&tenant).await;
}
