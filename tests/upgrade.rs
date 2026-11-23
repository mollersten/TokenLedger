//! Upgrading a database from migration 0005 (where production is) to the
//! latest: old-format rows (bucket holds and ledger bucket parts as JSON)
//! carry over into the new tables, and the old reservations keep working.

mod common;

use axum::http::StatusCode;
use backend::accounts;
use common::{Api, new_key, post};
use serde_json::json;
use sqlx::{PgPool, migrate::Migrator, postgres::PgPoolOptions};
use uuid::Uuid;

/// A throwaway database on the local test server, dropped at the end.
struct TempDb {
    admin: PgPool,
    name: String,
    url: String,
}

impl TempDb {
    async fn create() -> Self {
        let _ = dotenvy::dotenv();
        let base = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| common::DEFAULT_TEST_DATABASE_URL.to_owned());
        assert!(backend::is_local_database(&base), "upgrade tests only run against a local database");
        let (server, _) = base.rsplit_once('/').expect("database url has a database name");
        let admin = PgPoolOptions::new().max_connections(1).connect(&format!("{server}/postgres")).await.unwrap();
        let name = format!("tl_upgrade_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE DATABASE {name}")).execute(&admin).await.unwrap();
        Self { url: format!("{server}/{name}"), admin, name }
    }

    async fn drop(self) {
        sqlx::query(&format!("DROP DATABASE {} WITH (FORCE)", self.name)).execute(&self.admin).await.unwrap();
    }
}

fn migrator_up_to(version: i64) -> Migrator {
    Migrator {
        migrations: backend::MIGRATOR.migrations.iter().filter(|m| m.version <= version).cloned().collect(),
        ..Migrator::DEFAULT
    }
}

#[tokio::test]
async fn a_database_at_0005_with_old_format_rows_upgrades_and_keeps_working() {
    let db = TempDb::create().await;
    let pool = backend::connect(&db.url, 4).await.unwrap();
    migrator_up_to(5).run(&pool).await.unwrap();

    // State as the 0005 code left it: 10 expiring credits in one bucket, a
    // pending hold of 4 from it, and a reservation of 3 committed at 2.
    let (account, tenant) = ("acct_upgrade", "cus_upgrade");
    let (bucket, pending, committed) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let expires = chrono::Utc::now() + chrono::Duration::days(20);
    let mut tx = pool.begin().await.unwrap();
    for sql in [
        "INSERT INTO accounts (id, name, plan) VALUES ($1, 'upgrade test', 'paid')",
        "INSERT INTO wallets (account_id, tenant_id, available_credits, reserved_credits, expiring_credits, next_expiry_at) \
         VALUES ($1, $2, 4, 4, 4, $3)",
        "INSERT INTO credit_buckets (id, account_id, tenant_id, granted, remaining, expires_at, reason) \
         VALUES ($4, $1, $2, 10, 4, $3, 'monthly')",
    ] {
        sqlx::query(sql).bind(account).bind(tenant).bind(expires).bind(bucket).execute(&mut *tx).await.unwrap();
    }
    let hold = |amount: i64| json!([{ "bucket_id": bucket, "amount": amount, "expires_at": expires }]);
    let parts = |amount: i64| json!({ "buckets": [{ "bucket_id": bucket, "amount": amount }] });
    for (id, status, reserved, committed_amount, holds) in
        [(pending, "PENDING", 4, None, hold(4)), (committed, "COMMITTED", 3, Some(2_i64), hold(3))]
    {
        sqlx::query(
            "INSERT INTO reservations (id, account_id, tenant_id, feature, reserved_amount, committed_amount, \
                                       status, expires_at, bucket_holds) \
             VALUES ($1, $2, $3, 'report', $4, $5, $6, NOW() + INTERVAL '1 hour', $7)",
        )
        .bind(id).bind(account).bind(tenant).bind(reserved).bind(committed_amount).bind(status).bind(holds)
        .execute(&mut *tx).await.unwrap();
    }
    let entries = [
        ("GRANT", None, Some(bucket), 10, 0, 10, 0, json!({ "reason": "monthly" })),
        ("RESERVE", Some(pending), None, -4, 4, 6, 4, parts(4)),
        ("RESERVE", Some(committed), None, -3, 3, 3, 7, parts(3)),
        ("COMMIT", Some(committed), None, 0, -2, 3, 5, parts(2)),
        ("REFUND_UNUSED", Some(committed), None, 1, -1, 4, 4, parts(1)),
    ];
    for (i, (kind, reservation, bucket_id, da, dr, ba, br, metadata)) in entries.into_iter().enumerate() {
        sqlx::query(
            "INSERT INTO ledger_entries (id, account_id, tenant_id, reservation_id, bucket_id, entry_type, \
               delta_available, delta_reserved, balance_available_after, balance_reserved_after, metadata, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW() + make_interval(secs => $12))",
        )
        .bind(Uuid::new_v4()).bind(account).bind(tenant).bind(reservation).bind(bucket_id).bind(kind)
        .bind(da).bind(dr).bind(ba).bind(br).bind(metadata).bind(i as f64 / 1000.0)
        .execute(&mut *tx).await.unwrap();
    }
    tx.commit().await.unwrap();

    backend::migrate(&pool).await.expect("upgrade to the latest migration");

    let holds: Vec<(Uuid, i64)> =
        sqlx::query_as("SELECT reservation_id, amount FROM reservations_credit_buckets ORDER BY amount")
            .fetch_all(&pool).await.unwrap();
    assert_eq!(holds, vec![(committed, 3), (pending, 4)]);
    let moved: Vec<(String, i64)> = sqlx::query_as(
        "SELECT l.entry_type, p.amount FROM ledger_entries_credit_buckets p JOIN ledger_entries l ON l.id = p.ledger_entry_id \
         ORDER BY l.created_at",
    )
    .fetch_all(&pool).await.unwrap();
    assert_eq!(moved, vec![("RESERVE".into(), 4), ("RESERVE".into(), 3), ("COMMIT".into(), 2), ("REFUND_UNUSED".into(), 1)]);
    let column_left: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'reservations' AND column_name = 'bucket_holds')",
    )
    .fetch_one(&pool).await.unwrap();
    assert!(!column_left, "bucket_holds is dropped once copied");

    // The hold made before the upgrade commits through the new code, using
    // the bucket parts it now reads from the join table.
    let key = accounts::issue_key(&pool, account, None).await.unwrap();
    let api = Api { router: backend::router(backend::AppState::new(pool.clone())), api_key: Some(key.secret) };
    let res = post(&api, "/v1/credits/commit", &new_key(),
        json!({ "tenant_id": tenant, "reservation_id": pending, "actual_credits_used": 4 })).await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.body);
    let (available, reserved, expiring, remaining): (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT w.available_credits, w.reserved_credits, w.expiring_credits, b.remaining \
         FROM wallets w JOIN credit_buckets b ON b.account_id = w.account_id AND b.tenant_id = w.tenant_id",
    )
    .fetch_one(&pool).await.unwrap();
    assert_eq!((available, reserved, expiring, remaining), (4, 0, 4, 4));
    // granted 10 = remaining 4 + consumed (2 before the upgrade + 4 now).
    let consumed: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(p.amount), 0)::bigint FROM ledger_entries_credit_buckets p \
         JOIN ledger_entries l ON l.id = p.ledger_entry_id WHERE l.entry_type IN ('COMMIT', 'OVERAGE_DEDUCT')",
    )
    .fetch_one(&pool).await.unwrap();
    assert_eq!(consumed, 6);

    pool.close().await;
    db.drop().await;
}
