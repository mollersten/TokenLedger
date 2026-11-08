//! HTTP load generator for the TokenLedger API.
//!
//! Start the server in release mode first, then:
//!
//!   cargo run --release --example loadtest -- \
//!       --url http://127.0.0.1:8080 --concurrency 64 --duration 10
//!
//! Options:
//!   --url URL               API base URL               (default http://127.0.0.1:8080)
//!   --concurrency N[,N..]   in-flight requests          (default 64; a list runs a sweep)
//!   --duration SECS         per scenario                (default 10)
//!   --tenants N             wallets for spread-out load (default 1000)
//!   --scenario NAME[,..]    grant, reserve-commit, hot-tenant, replay, read, or all (default all)
//!
//! Every request carries a fresh Idempotency-Key (except `replay`, which
//! deliberately reuses one). After the run, every wallet the test touched is
//! reconciled against its ledger directly in PostgreSQL (TEST_DATABASE_URL,
//! defaulting to the local dev database).

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use uuid::Uuid;

const DEFAULT_DB_URL: &str = "postgres://postgres:postgres@localhost:5432/tokenledger";
const SEED_BALANCE: i64 = 1_000_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Grant,
    ReserveCommit,
    HotTenant,
    Replay,
    Read,
}

impl Scenario {
    const ALL: [Scenario; 5] =
        [Self::Grant, Self::ReserveCommit, Self::HotTenant, Self::Replay, Self::Read];

    fn name(self) -> &'static str {
        match self {
            Self::Grant => "grant",
            Self::ReserveCommit => "reserve-commit",
            Self::HotTenant => "hot-tenant",
            Self::Replay => "replay",
            Self::Read => "read",
        }
    }

    fn describe(self) -> &'static str {
        match self {
            Self::Grant => "POST grant, random tenant",
            Self::ReserveCommit => "reserve -> commit cycle, random tenant",
            Self::HotTenant => "reserve -> commit cycle, ONE shared tenant",
            Self::Replay => "POST reserve, same Idempotency-Key every time",
            Self::Read => "GET wallet + 20 ledger entries, random tenant",
        }
    }
}

struct Args {
    url: String,
    concurrency: Vec<usize>,
    duration: Duration,
    tenants: usize,
    scenarios: Vec<Scenario>,
}

fn parse_args() -> Args {
    let mut args = Args {
        url: "http://127.0.0.1:8080".into(),
        concurrency: vec![64],
        duration: Duration::from_secs(10),
        tenants: 1000,
        scenarios: Scenario::ALL.to_vec(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut value = || it.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--url" => args.url = value().trim_end_matches('/').to_owned(),
            "--concurrency" => {
                args.concurrency = value().split(',').map(|v| v.parse().expect("--concurrency")).collect()
            }
            "--duration" => args.duration = Duration::from_secs_f64(value().parse().expect("--duration")),
            "--tenants" => args.tenants = value().parse().expect("--tenants"),
            "--scenario" => {
                let v = value();
                if v != "all" {
                    args.scenarios = v
                        .split(',')
                        .map(|name| {
                            *Scenario::ALL
                                .iter()
                                .find(|s| s.name() == name)
                                .unwrap_or_else(|| panic!("unknown scenario {name}"))
                        })
                        .collect();
                }
            }
            "-h" | "--help" => {
                println!("see the doc comment at the top of examples/loadtest.rs");
                std::process::exit(0);
            }
            other => panic!("unknown flag {other}"),
        }
    }
    args
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Stats {
    /// op name -> latencies in microseconds (successful requests only)
    latencies: BTreeMap<&'static str, Vec<u32>>,
    /// op name -> successful request count
    ok: BTreeMap<&'static str, u64>,
    errors: u64,
    error_samples: Vec<String>,
}

impl Stats {
    fn record(&mut self, op: &'static str, started: Instant) {
        let micros = started.elapsed().as_micros().min(u32::MAX as u128) as u32;
        self.latencies.entry(op).or_default().push(micros);
        *self.ok.entry(op).or_default() += 1;
    }

    fn error(&mut self, msg: String) {
        self.errors += 1;
        if self.error_samples.len() < 3 {
            self.error_samples.push(msg);
        }
    }

    fn merge(&mut self, other: Stats) {
        for (op, mut v) in other.latencies {
            self.latencies.entry(op).or_default().append(&mut v);
        }
        for (op, n) in other.ok {
            *self.ok.entry(op).or_default() += n;
        }
        self.errors += other.errors;
        for s in other.error_samples {
            if self.error_samples.len() < 3 {
                self.error_samples.push(s);
            }
        }
    }
}

fn percentile(sorted: &[u32], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx] as f64 / 1000.0
}

/// Tiny xorshift PRNG so the example needs no `rand` dependency.
struct Rng(u64);

impl Rng {
    fn new() -> Self {
        Self(Uuid::new_v4().as_u128() as u64 | 1)
    }
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

struct Ctx {
    client: Client,
    url: String,
}

impl Ctx {
    async fn post(&self, path: &str, key: &str, body: &Value) -> Result<(StatusCode, Value), String> {
        let res = self
            .client
            .post(format!("{}{path}", self.url))
            .header("Idempotency-Key", key)
            .json(body)
            .send()
            .await
            .map_err(|e| format!("{path}: {e}"))?;
        let status = res.status();
        let body = res.json::<Value>().await.unwrap_or(Value::Null);
        Ok((status, body))
    }

    async fn get(&self, path: &str) -> Result<(StatusCode, Value), String> {
        let res = self
            .client
            .get(format!("{}{path}", self.url))
            .send()
            .await
            .map_err(|e| format!("{path}: {e}"))?;
        let status = res.status();
        let body = res.json::<Value>().await.unwrap_or(Value::Null);
        Ok((status, body))
    }
}

fn key() -> String {
    Uuid::new_v4().to_string()
}

async fn grant(ctx: &Ctx, tenant: &str, amount: i64) -> Result<(), String> {
    let (status, body) = ctx
        .post("/v1/credits/grant", &key(), &json!({ "tenant_id": tenant, "amount": amount, "reason": "loadtest" }))
        .await?;
    if status != StatusCode::OK {
        return Err(format!("grant {status}: {body}"));
    }
    Ok(())
}

/// One reserve -> commit cycle; records "reserve", "commit" and "cycle".
async fn reserve_commit(ctx: &Ctx, tenant: &str, rng: &mut Rng, stats: &mut Stats) {
    let cycle = Instant::now();
    let reserve = json!({
        "tenant_id": tenant,
        "feature": "loadtest",
        "estimate_credits": 10,
        "ttl_seconds": 300,
    });
    let reservation_id = match ctx.post("/v1/credits/reserve", &key(), &reserve).await {
        Ok((StatusCode::OK, body)) => {
            stats.record("reserve", cycle);
            body["reservation_id"].as_str().unwrap_or_default().to_owned()
        }
        Ok((status, body)) => return stats.error(format!("reserve {status}: {body}")),
        Err(e) => return stats.error(e),
    };
    let started = Instant::now();
    let commit = json!({
        "tenant_id": tenant,
        "reservation_id": reservation_id,
        "actual_credits_used": 1 + rng.below(10),
    });
    match ctx.post("/v1/credits/commit", &key(), &commit).await {
        Ok((StatusCode::OK, _)) => {
            stats.record("commit", started);
            stats.record("cycle", cycle);
        }
        Ok((status, body)) => stats.error(format!("commit {status}: {body}")),
        Err(e) => stats.error(e),
    }
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

struct Fixture {
    tenants: Arc<Vec<String>>,
    hot_tenant: String,
    replay_tenant: String,
    replay_key: String,
    replay_body: Value,
}

async fn seed(ctx: &Arc<Ctx>, prefix: &str, count: usize) -> Fixture {
    let tenants: Vec<String> = (0..count).map(|i| format!("{prefix}{i:05}")).collect();
    let all: Vec<String> = tenants
        .iter()
        .cloned()
        .chain([format!("{prefix}hot"), format!("{prefix}replay")])
        .collect();
    let started = Instant::now();
    let chunks: Vec<Vec<String>> = all.chunks(all.len().div_ceil(32)).map(<[String]>::to_vec).collect();
    let mut handles = Vec::new();
    for chunk in chunks {
        let ctx = ctx.clone();
        handles.push(tokio::spawn(async move {
            for t in chunk {
                grant(&ctx, &t, SEED_BALANCE).await.expect("seeding failed");
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    println!("seeded {} wallets in {:.1}s\n", all.len(), started.elapsed().as_secs_f64());

    let replay_tenant = format!("{prefix}replay");
    let replay_key = key();
    let replay_body = json!({
        "tenant_id": replay_tenant,
        "feature": "loadtest",
        "estimate_credits": 10,
        "ttl_seconds": 3600,
    });
    let (status, body) = ctx.post("/v1/credits/reserve", &replay_key, &replay_body).await.unwrap();
    assert_eq!(status, StatusCode::OK, "replay fixture reserve failed: {body}");

    Fixture {
        tenants: Arc::new(tenants),
        hot_tenant: format!("{prefix}hot"),
        replay_tenant,
        replay_key,
        replay_body,
    }
}

async fn run_scenario(
    ctx: &Arc<Ctx>,
    fixture: &Fixture,
    scenario: Scenario,
    concurrency: usize,
    duration: Duration,
) -> (Stats, Duration) {
    let started = Instant::now();
    let deadline = started + duration;
    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let ctx = ctx.clone();
        let tenants = fixture.tenants.clone();
        let hot = fixture.hot_tenant.clone();
        let replay_key = fixture.replay_key.clone();
        let replay_body = fixture.replay_body.clone();
        handles.push(tokio::spawn(async move {
            let mut stats = Stats::default();
            let mut rng = Rng::new();
            while Instant::now() < deadline {
                match scenario {
                    Scenario::Grant => {
                        let t = &tenants[rng.below(tenants.len())];
                        let s = Instant::now();
                        match grant(&ctx, t, 1).await {
                            Ok(()) => stats.record("grant", s),
                            Err(e) => stats.error(e),
                        }
                    }
                    Scenario::ReserveCommit => {
                        let t = tenants[rng.below(tenants.len())].clone();
                        reserve_commit(&ctx, &t, &mut rng, &mut stats).await;
                    }
                    Scenario::HotTenant => reserve_commit(&ctx, &hot, &mut rng, &mut stats).await,
                    Scenario::Replay => {
                        let s = Instant::now();
                        match ctx.post("/v1/credits/reserve", &replay_key, &replay_body).await {
                            Ok((StatusCode::OK, _)) => stats.record("reserve (replay)", s),
                            Ok((status, body)) => stats.error(format!("replay {status}: {body}")),
                            Err(e) => stats.error(e),
                        }
                    }
                    Scenario::Read => {
                        let t = &tenants[rng.below(tenants.len())];
                        let s = Instant::now();
                        match ctx.get(&format!("/v1/tenants/{t}/wallet")).await {
                            Ok((StatusCode::OK, _)) => stats.record("get wallet", s),
                            Ok((status, body)) => stats.error(format!("read {status}: {body}")),
                            Err(e) => stats.error(e),
                        }
                    }
                }
            }
            stats
        }));
    }
    let mut total = Stats::default();
    for h in handles {
        total.merge(h.await.unwrap());
    }
    (total, started.elapsed())
}

fn print_row(label: &str, conc: usize, lat: &mut [u32], elapsed: Duration) {
    lat.sort_unstable();
    println!(
        "  {label:<18} {conc:>5} {n:>9} {rps:>10.0} {p50:>8.2} {p90:>8.2} {p99:>8.2} {max:>9.2}",
        n = lat.len(),
        rps = lat.len() as f64 / elapsed.as_secs_f64(),
        p50 = percentile(lat, 50.0),
        p90 = percentile(lat, 90.0),
        p99 = percentile(lat, 99.0),
        max = percentile(lat, 100.0),
    );
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let max_conc = *args.concurrency.iter().max().unwrap();
    let client = Client::builder()
        .pool_max_idle_per_host(max_conc)
        .tcp_nodelay(true)
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let ctx = Arc::new(Ctx { client, url: args.url.clone() });

    match ctx.get("/healthz").await {
        Ok((StatusCode::OK, _)) => {}
        other => panic!("server not healthy at {}: {other:?}", args.url),
    }

    let run_id = &Uuid::new_v4().simple().to_string()[..8];
    let prefix = format!("lt_{run_id}_");
    println!("TokenLedger load test  run={run_id}  url={}  duration={:?}/scenario", args.url, args.duration);
    let fixture = seed(&ctx, &prefix, args.tenants).await;

    let mut totals: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut total_errors = 0;
    for &scenario in &args.scenarios {
        println!("{} — {}", scenario.name(), scenario.describe());
        println!(
            "  {:<18} {:>5} {:>9} {:>10} {:>8} {:>8} {:>8} {:>9}",
            "op", "conc", "requests", "req/s", "p50 ms", "p90 ms", "p99 ms", "max ms"
        );
        for &conc in &args.concurrency {
            let (mut stats, elapsed) = run_scenario(&ctx, &fixture, scenario, conc, args.duration).await;
            for (op, lat) in stats.latencies.iter_mut() {
                print_row(op, conc, lat, elapsed);
            }
            for (op, n) in &stats.ok {
                *totals.entry(op).or_default() += n;
            }
            if stats.errors > 0 {
                println!("  !! {} errors, e.g. {:?}", stats.errors, stats.error_samples);
            }
            total_errors += stats.errors;
        }
        println!();
    }

    verify(&ctx, &fixture, &prefix, &totals, args.tenants + 2).await;
    if total_errors > 0 {
        println!("FAILED: {total_errors} request errors");
        std::process::exit(1);
    }
}

/// Check that every wallet touched by this run reconciles with its ledger and
/// that the number of ledger entries matches the number of 200 responses.
async fn verify(
    ctx: &Ctx,
    fixture: &Fixture,
    prefix: &str,
    totals: &BTreeMap<&'static str, u64>,
    seeded: usize,
) {
    // Settle the replay fixture so no hold is left behind.
    let replay_id = fixture.replay_body["tenant_id"].as_str().unwrap();
    let (_, body) = ctx
        .post("/v1/credits/reserve", &fixture.replay_key, &fixture.replay_body)
        .await
        .unwrap();
    let rid = body["reservation_id"].as_str().unwrap();
    let (status, _) = ctx
        .post(
            "/v1/credits/commit",
            &key(),
            &json!({ "tenant_id": replay_id, "reservation_id": rid, "actual_credits_used": 10 }),
        )
        .await
        .unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.replay_tenant, replay_id);

    let _ = dotenvy::dotenv();
    let db_url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB_URL.to_owned());
    let pool = backend::connect(&db_url, 2).await.expect("connect for verification");

    let (wallets, mismatched, negative): (i64, i64, i64) = sqlx::query_as(
        "SELECT COUNT(*), \
                COUNT(*) FILTER (WHERE w.available_credits <> COALESCE(l.sa, 0) \
                                    OR w.reserved_credits <> COALESCE(l.sr, 0)), \
                COUNT(*) FILTER (WHERE w.available_credits < 0 OR w.reserved_credits < 0) \
         FROM wallets w \
         LEFT JOIN (SELECT tenant_id, SUM(delta_available)::bigint sa, SUM(delta_reserved)::bigint sr \
                    FROM ledger_entries WHERE starts_with(tenant_id, $1) GROUP BY tenant_id) l \
           USING (tenant_id) \
         WHERE starts_with(w.tenant_id, $1)",
    )
    .bind(prefix)
    .fetch_one(&pool)
    .await
    .unwrap();

    let entries: Vec<(String, i64)> = sqlx::query_as(
        "SELECT entry_type, COUNT(*) FROM ledger_entries WHERE starts_with(tenant_id, $1) GROUP BY 1",
    )
    .bind(prefix)
    .fetch_all(&pool)
    .await
    .unwrap();
    let entries: BTreeMap<String, i64> = entries.into_iter().collect();
    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reservations WHERE starts_with(tenant_id, $1) AND status = 'PENDING'",
    )
    .bind(prefix)
    .fetch_one(&pool)
    .await
    .unwrap();

    let get = |m: &BTreeMap<String, i64>, k: &str| m.get(k).copied().unwrap_or(0) as u64;
    let ok = |k: &str| totals.get(k).copied().unwrap_or(0);
    // +1 RESERVE and +1 COMMIT for the replay fixture, +seeded GRANTs.
    let expected_grants = ok("grant") + seeded as u64;
    let expected_reserves = ok("reserve") + 1;
    let expected_commits = ok("commit") + 1;

    println!("verification");
    println!("  wallets checked            {wallets}");
    println!("  wallet != SUM(ledger)      {mismatched}");
    println!("  negative balances          {negative}");
    println!("  pending reservations left  {pending}");
    println!(
        "  GRANT entries   {:>9}  (expected {expected_grants})",
        get(&entries, "GRANT")
    );
    println!(
        "  RESERVE entries {:>9}  (expected {expected_reserves}; replays add none)",
        get(&entries, "RESERVE")
    );
    println!(
        "  COMMIT entries  {:>9}  (expected {expected_commits})",
        get(&entries, "COMMIT")
    );
    let consistent = mismatched == 0
        && negative == 0
        && pending == 0
        && get(&entries, "GRANT") == expected_grants
        && get(&entries, "RESERVE") == expected_reserves
        && get(&entries, "COMMIT") == expected_commits;
    println!("  => {}", if consistent { "CONSISTENT" } else { "INCONSISTENT" });
    if !consistent {
        std::process::exit(1);
    }
}
