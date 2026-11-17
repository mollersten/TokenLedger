#!/usr/bin/env bash
# Run a command against the local database instead of the real one.
#
# backend/.env points DATABASE_URL at Supabase, so plain `cargo run` and the
# admin CLI work on real data. Use this for local server runs, smoke tests
# and load tests:
#
#   scripts/with-local-db.sh cargo run --release
#   scripts/with-local-db.sh cargo run --bin admin -- list
#
# It sets DATABASE_URL to TEST_DATABASE_URL if that is set in the shell,
# else to the local dev database from scripts/local-pg.sh. Variables set
# here win over backend/.env (dotenvy never overrides them), so the command
# never sees the Supabase connection string.
set -euo pipefail

export DATABASE_URL="${TEST_DATABASE_URL:-postgres://postgres:postgres@localhost:5432/tokenledger}"
export DATABASE_MAX_CONNECTIONS="${LOCAL_DATABASE_MAX_CONNECTIONS:-20}"
exec "$@"
