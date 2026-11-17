//! Account and API key administration for TokenLedger.
//!
//! Uses DATABASE_URL (from the environment or backend/.env, where it points at
//! the real database on Supabase) and applies any pending migrations first.
//! Prints the database it uses to stderr when it connects. To work on the
//! local database instead, prefix the command with `scripts/with-local-db.sh`.
//!
//! With no arguments it opens an interactive shell: it connects once, then
//! runs one command per line until `exit` or Ctrl-D, with line editing and
//! history (kept in ~/.tokenledger_admin_history):
//!
//!   cargo run -q --bin admin
//!   admin@supabase> list
//!   admin@supabase> create-account --name "Acme AI" --plan free
//!
//! With arguments it runs that one command and exits:
//!
//!   cargo run -q --bin admin -- list
//!   scripts/with-local-db.sh cargo run -q --bin admin -- list
//!
//! The commands and their flags are in `HELP` below (`help` prints them).

use std::{collections::HashMap, io::IsTerminal, path::PathBuf, process::ExitCode};

use backend::accounts::{self, Account, IssuedKey, LimitChange, NewAccount, Plan};
use chrono::{DateTime, Duration, Utc};
use rustyline::{DefaultEditor, error::ReadlineError};
use sqlx::PgPool;

const HELP: &str = "\
Commands:
  create-account --name NAME [--plan free|trial|paid] [--trial-days N]
                 [--monthly-requests N|unlimited] [--hourly-requests N|unlimited]
                 [--grace-hourly-requests N|unlimited] [--max-tenants N|unlimited]
                 [--rate-limit N|unlimited] [--key-label LABEL] [--no-key]
      Free: 120 requests/hour (600/hour for 24 h after the first time it
      goes over), 10 req/s, no end date.
      Trial (the default): 14 days, 100000 requests/month, 1000 tenants, 50 req/s.
      Paid: unlimited. Issues a first API key unless --no-key.
  create-key    --account ID [--label LABEL]
  revoke-key    --key KEY_ID
  accounts
      Every account's id and name.
  list
      Every account with its plan, status, keys, tenants, usage and limits.
  keys          --account ID
  suspend       --account ID
  activate      --account ID
  extend-trial  --account ID --days N
  set-plan      --account ID --plan free|trial|paid [--keep-limits]
      Applies the plan's limits (an upgrade to paid lifts them all) unless
      --keep-limits.
  set-limits    --account ID [--monthly-requests N|unlimited]
                [--hourly-requests N|unlimited] [--grace-hourly-requests N|unlimited]
                [--max-tenants N|unlimited] [--rate-limit N|unlimited]
  extend-grace  --account ID --hours N
      Gives N more hours at the grace limit (starting now if it is over or
      has not started).
  reset-grace   --account ID
      Lets the next request over the hourly limit start a new grace period.

Changes reach running servers within their auth cache TTL (30 s default).
Values with spaces need quotes: --name \"Acme AI\".";

const SHELL_HELP: &str = "In the shell: help, exit (or Ctrl-D). Ctrl-C clears the line.";

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

/// Connects to DATABASE_URL and applies pending migrations. Returns the pool
/// and the URL (for the shell prompt).
async fn connect() -> Result<(PgPool, String), String> {
    let _ = dotenvy::dotenv();
    let url = std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL must be set".to_owned())?;
    eprintln!("database: {}", backend::database_label(&url));
    let pool = backend::connect(&url, 2).await.map_err(|e| format!("cannot connect: {e}"))?;
    backend::MIGRATOR.run(&pool).await.map_err(|e| format!("migrations failed: {e}"))?;
    Ok((pool, url))
}

/// Splits a shell line into words: whitespace separates them, single and
/// double quotes group them, and a backslash escapes the next character
/// (except inside single quotes).
fn split_words(line: &str) -> Result<Vec<String>, String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut in_word = false;
    let mut quote: Option<char> = None;
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        match quote {
            Some(q) if c == q => quote = None,
            Some('"') if c == '\\' => word.extend(chars.next()),
            Some(_) => word.push(c),
            None if c == '"' || c == '\'' => {
                quote = Some(c);
                in_word = true;
            }
            None if c == '\\' => {
                word.extend(chars.next());
                in_word = true;
            }
            None if c.is_whitespace() => {
                if in_word {
                    words.push(std::mem::take(&mut word));
                    in_word = false;
                }
            }
            None => {
                word.push(c);
                in_word = true;
            }
        }
    }
    if quote.is_some() {
        return Err("unclosed quote".to_owned());
    }
    if in_word {
        words.push(word);
    }
    Ok(words)
}

/// A shell line as a command and its arguments. Also accepts a pasted
/// one-shot command (`admin list`, `cargo run -q --bin admin -- list`).
fn shell_command(words: &[String]) -> Option<(&str, &[String])> {
    let words = match words.first().map(String::as_str) {
        Some("admin") => &words[1..],
        Some("cargo") => match words.iter().position(|w| w == "--") {
            Some(i) => &words[i + 1..],
            None => &words[words.len()..],
        },
        _ => words,
    };
    words.split_first().map(|(c, rest)| (c.as_str(), rest))
}

/// Short name of the database for the prompt.
fn prompt_name(url: &str) -> String {
    let label = backend::database_label(url);
    if backend::is_local_database(url) {
        "local".to_owned()
    } else if label.contains(".supabase.com") || label.contains(".supabase.co") {
        "supabase".to_owned()
    } else {
        label.split(':').next().unwrap_or(&label).to_owned()
    }
}

fn history_file() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".tokenledger_admin_history"))
}

async fn shell(pool: &PgPool, url: &str) -> ExitCode {
    let mut editor = match DefaultEditor::new() {
        Ok(editor) => editor,
        Err(e) => {
            eprintln!("error: cannot start the shell: {e}");
            return ExitCode::FAILURE;
        }
    };
    // Piped input (scripts, tests) neither reads nor writes the history.
    let history = history_file().filter(|_| std::io::stdin().is_terminal());
    if let Some(path) = &history {
        let _ = editor.load_history(path);
    }
    let prompt = format!("admin@{}> ", prompt_name(url));
    println!("TokenLedger admin shell. Type `help` for commands. {SHELL_HELP}");

    loop {
        // Reading a line blocks; let the runtime move other tasks off this thread.
        let line = match tokio::task::block_in_place(|| editor.readline(&prompt)) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => continue,
            Err(ReadlineError::Eof) => break,
            Err(e) => {
                eprintln!("error: {e}");
                return ExitCode::FAILURE;
            }
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let _ = editor.add_history_entry(line);
        if let Some(path) = &history {
            let _ = editor.save_history(path);
        }
        let words = match split_words(line) {
            Ok(words) => words,
            Err(e) => {
                eprintln!("error: {e}");
                continue;
            }
        };
        let Some((command, args)) = shell_command(&words) else { continue };
        let result = match command {
            "exit" | "quit" => break,
            "help" | "?" | "--help" | "-h" => {
                println!("{HELP}\n\n{SHELL_HELP}");
                Ok(())
            }
            _ => match Flags::parse(args) {
                Ok(flags) => run(pool, command, &flags).await,
                Err(e) => Err(e),
            },
        };
        if let Err(e) = result {
            eprintln!("error: {e}");
        }
    }
    ExitCode::SUCCESS
}

async fn run(pool: &PgPool, command: &str, flags: &Flags) -> Result<(), String> {
    let pool = pool.clone();
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
        "accounts" => {
            let names = accounts::list_account_names(&pool).await.map_err(db)?;
            if names.is_empty() {
                println!("No accounts yet.");
            } else {
                let width = names.iter().map(|(id, _)| id.len()).max().unwrap_or(0).max("id".len());
                println!("{:<width$}  name", "id");
                for (id, name) in &names {
                    println!("{id:<width$}  {name}");
                }
                println!("{} account{}", names.len(), if names.len() == 1 { "" } else { "s" });
            }
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
        other => return Err(format!("unknown command '{other}' (type `help` for the list)")),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Some(first) = args.first()
        && matches!(first.as_str(), "help" | "-h" | "--help")
    {
        println!("usage: admin                      (interactive shell)\n       admin <command> [--flag value ...]\n\n{HELP}");
        return ExitCode::SUCCESS;
    }
    let (pool, url) = match connect().await {
        Ok(connected) => connected,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };
    let Some((command, rest)) = args.split_first() else {
        return shell(&pool, &url).await;
    };
    let result = match Flags::parse(rest) {
        Ok(flags) => run(&pool, command, &flags).await,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn words(line: &str) -> Vec<String> {
        split_words(line).unwrap()
    }

    #[test]
    fn splits_like_a_shell() {
        assert_eq!(words("  list  "), ["list"]);
        assert_eq!(
            words(r#"create-account --name "Acme AI" --plan free"#),
            ["create-account", "--name", "Acme AI", "--plan", "free"]
        );
        assert_eq!(words("--name 'O\"Neil Co'"), ["--name", "O\"Neil Co"]);
        assert_eq!(words(r#"--name "say \"hi\"" --x a\ b"#), ["--name", "say \"hi\"", "--x", "a b"]);
        assert_eq!(words(r#"--label """#), ["--label", ""]);
        assert!(words("").is_empty());
        assert_eq!(split_words(r#"--name "Acme"#), Err("unclosed quote".to_owned()));
    }

    #[test]
    fn accepts_pasted_one_shot_commands() {
        for line in ["list", "admin list", "cargo run -q --bin admin -- list"] {
            let w = words(line);
            assert_eq!(shell_command(&w).map(|(c, _)| c), Some("list"), "{line}");
        }
        let w = words("cargo run --bin admin -- keys --account acct_1");
        let (command, args) = shell_command(&w).unwrap();
        assert_eq!((command, args.len()), ("keys", 2));
        assert_eq!(shell_command(&words("cargo build")), None);
    }

    #[test]
    fn prompt_names_the_database() {
        assert_eq!(prompt_name("postgres://postgres:postgres@localhost:5432/tokenledger"), "local");
        assert_eq!(
            prompt_name("postgresql://postgres.abc:pw@aws-0-eu-west-1.pooler.supabase.com:5432/postgres"),
            "supabase"
        );
        assert_eq!(prompt_name("postgres://u:p@db.example.com:5432/app"), "db.example.com");
    }
}
