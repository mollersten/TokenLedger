//! Account and API key administration for TokenLedger.
//!
//! Uses DATABASE_URL (from the environment or backend/.env, where it points at
//! the real database on Supabase) and applies any pending migrations first.
//! Prints the database it uses to stderr before every command. To work on the
//! local database instead, prefix the command with `scripts/with-local-db.sh`.
//!
//!   cargo run --bin admin -- create-account --name "Acme AI"
//!   cargo run --bin admin -- list
//!   scripts/with-local-db.sh cargo run --bin admin -- list
//!
//! Commands:
//!   create-account --name NAME [--plan free|trial|paid] [--trial-days N]
//!                  [--monthly-requests N|unlimited] [--hourly-requests N|unlimited]
//!                  [--grace-hourly-requests N|unlimited] [--max-tenants N|unlimited]
//!                  [--rate-limit N|unlimited] [--key-label LABEL] [--no-key]
//!       Free: 120 requests/hour (600/hour for 24 h after the first time it
//!       goes over), 10 req/s, no end date.
//!       Trial (the default): 14 days, 100000 requests/month, 1000 tenants, 50 req/s.
//!       Paid: unlimited. Issues a first API key unless --no-key.
//!   create-key    --account ID [--label LABEL]
//!   revoke-key    --key KEY_ID
//!   list
//!   keys          --account ID
//!   suspend       --account ID
//!   activate      --account ID
//!   extend-trial  --account ID --days N
//!   set-plan      --account ID --plan free|trial|paid [--keep-limits]
//!       Applies the plan's limits (an upgrade to paid lifts them all) unless
//!       --keep-limits.
//!   set-limits    --account ID [--monthly-requests N|unlimited]
//!                 [--hourly-requests N|unlimited] [--grace-hourly-requests N|unlimited]
//!                 [--max-tenants N|unlimited] [--rate-limit N|unlimited]
//!   extend-grace  --account ID --hours N
//!       Gives N more hours at the grace limit (starting now if it is over or
//!       has not started).
//!   reset-grace   --account ID
//!       Lets the next request over the hourly limit start a new grace period.
//!
//! Changes reach running servers within their auth cache TTL (30 s default).

use std::{collections::HashMap, process::ExitCode};

use backend::accounts::{self, Account, IssuedKey, LimitChange, NewAccount, Plan};
use chrono::{DateTime, Duration, Utc};

const USAGE: &str = "usage: admin <create-account|create-key|revoke-key|list|keys|suspend|activate|extend-trial|set-plan|set-limits|extend-grace|reset-grace> [--flag value ...]
see the doc comment in src/bin/admin.rs for every flag";

struct Flags(HashMap<String, Option<String>>);

impl Flags {
    fn parse(args: &[String]) -> Result<Self, String> {
        let mut map = HashMap::new();
        let mut i = 0;
        while i < args.len() {
            let name = args[i].strip_prefix("--").ok_or_else(|| format!("unexpected argument '{}'", args[i]))?;
            let value = args.get(i + 1).filter(|v| !v.starts_with("--")).cloned();
            i += if value.is_some() { 2 } else { 1 };
            map.insert(name.to_owned(), value);
        }
        Ok(Self(map))
    }

    fn has(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    fn opt(&self, name: &str) -> Option<&str> {
        self.0.get(name).and_then(|v| v.as_deref())
    }

    fn req(&self, name: &str) -> Result<&str, String> {
        self.opt(name).ok_or_else(|| format!("--{name} is required"))
    }

    /// `Some(None)` for "unlimited", `Some(Some(n))` for a number, `None` if absent.
    fn limit<T: std::str::FromStr>(&self, name: &str) -> Result<Option<Option<T>>, String> {
        match self.opt(name) {
            None if self.has(name) => Err(format!("--{name} needs a value")),
            None => Ok(None),
            Some("unlimited" | "none") => Ok(Some(None)),
            Some(v) => v
                .parse()
                .map(|n| Some(Some(n)))
                .map_err(|_| format!("--{name} must be a number or 'unlimited'")),
        }
    }
}

fn print_key(key: &IssuedKey) {
    println!("API key {} for {}:", key.id, key.account_id);
    println!();
    println!("    {}", key.secret);
    println!();
    println!("Store it now: only its hash is kept, so it cannot be shown again.");
}

fn fmt_limit<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map_or_else(|| "unlimited".to_owned(), |v| v.to_string())
}

fn fmt_time(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d %H:%M UTC").to_string()
}

fn plan_flag(value: &str) -> Result<Plan, String> {
    Plan::parse(value).ok_or_else(|| "--plan must be free, trial or paid".to_owned())
}

/// Short grace-period state for `list`.
fn grace_state(a: &Account) -> String {
    match (a.grace_hourly_request_limit, a.grace_ends_at) {
        (None, _) => "-".to_owned(),
        (Some(_), None) => "unused".to_owned(),
        (Some(_), Some(ends)) if ends > Utc::now() => format!("until {}", ends.format("%m-%d %H:%M")),
        (Some(_), Some(_)) => "used".to_owned(),
    }
}

fn print_limits(a: &Account) {
    if let Some(ends) = a.trial_ends_at {
        println!("  trial ends        {}", fmt_time(ends));
    }
    println!("  requests/month    {}", fmt_limit(a.monthly_request_limit));
    println!("  requests/hour     {}", fmt_limit(a.hourly_request_limit));
    if a.hourly_request_limit.is_some() {
        println!(
            "  grace period      {}",
            match (a.grace_hourly_request_limit, a.grace_ends_at) {
                (None, _) => "none".to_owned(),
                (Some(g), None) => format!("{g} requests/hour for {} h, not started", accounts::GRACE_PERIOD_HOURS),
                (Some(g), Some(ends)) if ends > Utc::now() => format!("{g} requests/hour until {}", fmt_time(ends)),
                (Some(_), Some(ends)) => format!("used, ended {}", fmt_time(ends)),
            }
        );
    }
    println!("  max tenants       {}", fmt_limit(a.max_tenants));
    println!("  rate limit/sec    {}", fmt_limit(a.rate_limit_per_sec));
}

fn limit_change(flags: &Flags) -> Result<LimitChange, String> {
    Ok(LimitChange {
        monthly_request_limit: flags.limit("monthly-requests")?,
        hourly_request_limit: flags.limit("hourly-requests")?,
        grace_hourly_request_limit: flags.limit("grace-hourly-requests")?,
        max_tenants: flags.limit("max-tenants")?,
        rate_limit_per_sec: flags.limit("rate-limit")?,
    })
}

async fn run(command: &str, flags: &Flags) -> Result<(), String> {
    let _ = dotenvy::dotenv();
    let url = std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL must be set".to_owned())?;
    eprintln!("database: {}", backend::database_label(&url));
    let pool = backend::connect(&url, 2).await.map_err(|e| format!("cannot connect: {e}"))?;
    backend::MIGRATOR.run(&pool).await.map_err(|e| format!("migrations failed: {e}"))?;
    let db = |e: sqlx::Error| format!("database error: {e}");
    let found = |ok: bool, what: &str| if ok { Ok(()) } else { Err(format!("{what} not found")) };

    match command {
        "create-account" => {
            let name = flags.req("name")?;
            let plan = plan_flag(flags.opt("plan").unwrap_or("trial"))?;
            let mut new = NewAccount::on_plan(name, plan);
            if let Some(days) = flags.opt("trial-days") {
                let days: i64 = days.parse().map_err(|_| "--trial-days must be a number")?;
                new.trial_ends_at = Some(Utc::now() + Duration::days(days));
            }
            let change = limit_change(flags)?;
            if let Some(v) = change.monthly_request_limit {
                new.monthly_request_limit = v;
            }
            if let Some(v) = change.hourly_request_limit {
                new.hourly_request_limit = v;
            }
            if let Some(v) = change.grace_hourly_request_limit {
                new.grace_hourly_request_limit = v;
            }
            if let Some(v) = change.max_tenants {
                new.max_tenants = v;
            }
            if let Some(v) = change.rate_limit_per_sec {
                new.rate_limit_per_sec = v;
            }
            let account = accounts::create_account(&pool, &new).await.map_err(db)?;
            println!("Created account {} ({}, plan {})", account.id, account.name, account.plan);
            print_limits(&account);
            if !flags.has("no-key") {
                println!();
                let key = accounts::issue_key(&pool, &account.id, flags.opt("key-label")).await.map_err(db)?;
                print_key(&key);
            }
        }
        "create-key" => {
            let account_id = flags.req("account")?;
            accounts::get_account(&pool, account_id).await.map_err(db)?.ok_or("account not found")?;
            let key = accounts::issue_key(&pool, account_id, flags.opt("label")).await.map_err(db)?;
            print_key(&key);
        }
        "revoke-key" => {
            let key_id = flags.req("key")?;
            found(accounts::revoke_key(&pool, key_id).await.map_err(db)?, "active key")?;
            println!("Revoked {key_id}. Running servers reject it within 30 s.");
        }
        "list" => {
            let all = accounts::list_accounts(&pool).await.map_err(db)?;
            println!(
                "{:<26} {:<24} {:<6} {:<10} {:<11} {:>5} {:>8} {:>14} {:>12} {:<17}",
                "account", "name", "plan", "status", "trial ends", "keys", "tenants", "requests/mo", "requests/h",
                "grace"
            );
            for s in all {
                let a = &s.account;
                println!(
                    "{:<26} {:<24} {:<6} {:<10} {:<11} {:>5} {:>8} {:>14} {:>12} {:<17}",
                    a.id,
                    a.name.chars().take(24).collect::<String>(),
                    a.plan,
                    a.status,
                    a.trial_ends_at.map_or("-".to_owned(), |t| t.format("%Y-%m-%d").to_string()),
                    s.active_keys,
                    s.tenants,
                    format!("{}/{}", s.requests_this_month, fmt_limit(a.monthly_request_limit)),
                    format!("{}/{}", s.requests_this_hour, fmt_limit(a.hourly_request_limit)),
                    grace_state(a),
                );
            }
        }
        "keys" => {
            let account_id = flags.req("account")?;
            for k in accounts::list_keys(&pool, account_id).await.map_err(db)? {
                println!(
                    "{:<24} {:<20} {:<10} created {}  {}",
                    k.id,
                    k.key_prefix,
                    k.label.unwrap_or_default(),
                    k.created_at.format("%Y-%m-%d"),
                    k.revoked_at.map_or("active".to_owned(), |t| format!("revoked {}", t.format("%Y-%m-%d"))),
                );
            }
        }
        "suspend" | "activate" => {
            let account_id = flags.req("account")?;
            found(accounts::set_status(&pool, account_id, command == "activate").await.map_err(db)?, "account")?;
            println!("{account_id} is now {}", if command == "activate" { "active" } else { "suspended" });
        }
        "extend-trial" => {
            let account_id = flags.req("account")?;
            let days: i64 = flags.req("days")?.parse().map_err(|_| "--days must be a number")?;
            let ends = accounts::extend_trial(&pool, account_id, days).await.map_err(db)?.ok_or("account not found")?;
            println!("Trial for {account_id} now ends {}", ends.format("%Y-%m-%d %H:%M UTC"));
        }
        "set-plan" => {
            let account_id = flags.req("account")?;
            let plan = plan_flag(flags.req("plan")?)?;
            let keep_limits = flags.has("keep-limits");
            found(accounts::set_plan(&pool, account_id, plan, keep_limits).await.map_err(db)?, "account")?;
            let a = accounts::get_account(&pool, account_id).await.map_err(db)?.ok_or("account not found")?;
            println!("{account_id} is now on the {} plan (running servers apply it within 30 s)", plan.as_str());
            print_limits(&a);
        }
        "set-limits" => {
            let account_id = flags.req("account")?;
            found(accounts::set_limits(&pool, account_id, &limit_change(flags)?).await.map_err(db)?, "account")?;
            let a = accounts::get_account(&pool, account_id).await.map_err(db)?.ok_or("account not found")?;
            println!("{account_id}:");
            print_limits(&a);
        }
        "extend-grace" => {
            let account_id = flags.req("account")?;
            let hours: i32 = flags.req("hours")?.parse().map_err(|_| "--hours must be a number")?;
            let ends = accounts::extend_grace(&pool, account_id, hours).await.map_err(db)?.ok_or("account not found")?;
            println!("Grace period for {account_id} now ends {}", fmt_time(ends));
        }
        "reset-grace" => {
            let account_id = flags.req("account")?;
            found(accounts::set_grace_end(&pool, account_id, None).await.map_err(db)?, "account")?;
            println!("{account_id} will get a new grace period the next time it goes over its hourly limit");
        }
        other => return Err(format!("unknown command '{other}'\n{USAGE}")),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some((command, rest)) = args.split_first() else {
        eprintln!("{USAGE}");
        return ExitCode::FAILURE;
    };
    if command == "-h" || command == "--help" {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    let result = match Flags::parse(rest) {
        Ok(flags) => run(command, &flags).await,
        Err(e) => Err(e),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
