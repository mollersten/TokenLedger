//! Account and API key administration for TokenLedger.
//!
//! Uses DATABASE_URL (from the environment or backend/.env) and applies any
//! pending migrations first.
//!
//!   cargo run --bin admin -- create-account --name "Acme AI"
//!   cargo run --bin admin -- list
//!
//! Commands:
//!   create-account --name NAME [--plan trial|paid] [--trial-days N]
//!                  [--monthly-requests N|unlimited] [--max-tenants N|unlimited]
//!                  [--rate-limit N|unlimited] [--key-label LABEL] [--no-key]
//!       Trial defaults: 14 days, 100000 requests/month, 1000 tenants, 50 req/s.
//!       Paid accounts default to unlimited. Issues a first API key unless --no-key.
//!   create-key    --account ID [--label LABEL]
//!   revoke-key    --key KEY_ID
//!   list
//!   keys          --account ID
//!   suspend       --account ID
//!   activate      --account ID
//!   extend-trial  --account ID --days N
//!   set-plan      --account ID --plan trial|paid
//!   set-limits    --account ID [--monthly-requests N|unlimited]
//!                 [--max-tenants N|unlimited] [--rate-limit N|unlimited]
//!
//! Changes reach running servers within their auth cache TTL (30 s default).

use std::{collections::HashMap, process::ExitCode};

use backend::accounts::{self, IssuedKey, LimitChange, NewAccount, Plan};
use chrono::{Duration, Utc};

const USAGE: &str = "usage: admin <create-account|create-key|revoke-key|list|keys|suspend|activate|extend-trial|set-plan|set-limits> [--flag value ...]
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

async fn run(command: &str, flags: &Flags) -> Result<(), String> {
    let _ = dotenvy::dotenv();
    let url = std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL must be set".to_owned())?;
    let pool = backend::connect(&url, 2).await.map_err(|e| format!("cannot connect: {e}"))?;
    backend::MIGRATOR.run(&pool).await.map_err(|e| format!("migrations failed: {e}"))?;
    let db = |e: sqlx::Error| format!("database error: {e}");
    let found = |ok: bool, what: &str| if ok { Ok(()) } else { Err(format!("{what} not found")) };

    match command {
        "create-account" => {
            let name = flags.req("name")?;
            let plan = Plan::parse(flags.opt("plan").unwrap_or("trial")).ok_or("--plan must be trial or paid")?;
            let mut new = match plan {
                Plan::Trial => NewAccount::trial(name),
                Plan::Paid => NewAccount::unlimited(name),
            };
            if let Some(days) = flags.opt("trial-days") {
                let days: i64 = days.parse().map_err(|_| "--trial-days must be a number")?;
                new.trial_ends_at = Some(Utc::now() + Duration::days(days));
            }
            if let Some(v) = flags.limit("monthly-requests")? {
                new.monthly_request_limit = v;
            }
            if let Some(v) = flags.limit("max-tenants")? {
                new.max_tenants = v;
            }
            if let Some(v) = flags.limit("rate-limit")? {
                new.rate_limit_per_sec = v;
            }
            let account = accounts::create_account(&pool, &new).await.map_err(db)?;
            println!("Created account {} ({}, plan {})", account.id, account.name, account.plan);
            if let Some(ends) = account.trial_ends_at {
                println!("  trial ends        {}", ends.format("%Y-%m-%d %H:%M UTC"));
            }
            println!("  requests/month    {}", fmt_limit(account.monthly_request_limit));
            println!("  max tenants       {}", fmt_limit(account.max_tenants));
            println!("  rate limit/sec    {}", fmt_limit(account.rate_limit_per_sec));
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
                "{:<26} {:<24} {:<6} {:<10} {:<11} {:>5} {:>8} {:>14}",
                "account", "name", "plan", "status", "trial ends", "keys", "tenants", "requests/mo"
            );
            for s in all {
                let a = &s.account;
                println!(
                    "{:<26} {:<24} {:<6} {:<10} {:<11} {:>5} {:>8} {:>14}",
                    a.id,
                    a.name.chars().take(24).collect::<String>(),
                    a.plan,
                    a.status,
                    a.trial_ends_at.map_or("-".to_owned(), |t| t.format("%Y-%m-%d").to_string()),
                    s.active_keys,
                    s.tenants,
                    format!("{}/{}", s.requests_this_month, fmt_limit(a.monthly_request_limit)),
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
            let plan = Plan::parse(flags.req("plan")?).ok_or("--plan must be trial or paid")?;
            found(accounts::set_plan(&pool, account_id, plan).await.map_err(db)?, "account")?;
            println!("{account_id} is now on the {} plan", plan.as_str());
        }
        "set-limits" => {
            let account_id = flags.req("account")?;
            let change = LimitChange {
                monthly_request_limit: flags.limit("monthly-requests")?,
                max_tenants: flags.limit("max-tenants")?,
                rate_limit_per_sec: flags.limit("rate-limit")?,
            };
            found(accounts::set_limits(&pool, account_id, &change).await.map_err(db)?, "account")?;
            let a = accounts::get_account(&pool, account_id).await.map_err(db)?.ok_or("account not found")?;
            println!(
                "{account_id}: requests/month {}, max tenants {}, rate limit/sec {}",
                fmt_limit(a.monthly_request_limit),
                fmt_limit(a.max_tenants),
                fmt_limit(a.rate_limit_per_sec)
            );
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
