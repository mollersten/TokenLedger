#!/usr/bin/env bash
# Throwaway local PostgreSQL 16 for development and `cargo test`, without Docker.
#
#   scripts/local-pg.sh start    # init (first run) + start on localhost:5432
#   scripts/local-pg.sh stop
#   scripts/local-pg.sh status
#   scripts/local-pg.sh reset    # stop, delete all data, start fresh
#
# Matches docker-compose.yml: postgres://postgres:postgres@localhost:5432/tokenledger
# Data lives in backend/.pgdata (git-ignored). Uses Homebrew's postgresql@16.
set -euo pipefail
export LC_ALL=C LANG=C

PG_BIN="${PG_BIN:-/opt/homebrew/opt/postgresql@16/bin}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PGDATA="$ROOT/.pgdata"
PORT="${PGPORT:-5432}"
DB_NAME="tokenledger"

init() {
  [ -f "$PGDATA/PG_VERSION" ] && return
  echo "Initializing cluster in $PGDATA"
  local pwfile
  pwfile="$(mktemp)"
  printf 'postgres\n' > "$pwfile"
  "$PG_BIN/initdb" -D "$PGDATA" -U postgres --pwfile="$pwfile" \
    --auth-local=trust --auth-host=scram-sha-256 --encoding=UTF8 --no-locale >/dev/null
  rm -f "$pwfile"
}

start() {
  init
  if "$PG_BIN/pg_ctl" -D "$PGDATA" status >/dev/null 2>&1; then
    echo "Already running on port $PORT"
  else
    "$PG_BIN/pg_ctl" -D "$PGDATA" -l "$PGDATA/server.log" -w \
      -o "-p $PORT -c listen_addresses=localhost -c unix_socket_directories='$PGDATA'" start >/dev/null
    echo "Started PostgreSQL 16 on localhost:$PORT"
  fi
  if ! "$PG_BIN/psql" -h "$PGDATA" -p "$PORT" -U postgres -tAc \
      "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'" postgres | grep -q 1; then
    "$PG_BIN/createdb" -h "$PGDATA" -p "$PORT" -U postgres "$DB_NAME"
    echo "Created database $DB_NAME"
  fi
}

stop() {
  if "$PG_BIN/pg_ctl" -D "$PGDATA" status >/dev/null 2>&1; then
    "$PG_BIN/pg_ctl" -D "$PGDATA" -m fast -w stop >/dev/null
    echo "Stopped"
  else
    echo "Not running"
  fi
}

case "${1:-start}" in
  start)  start ;;
  stop)   stop ;;
  status) "$PG_BIN/pg_ctl" -D "$PGDATA" status || true ;;
  reset)  stop; rm -rf "$PGDATA"; start ;;
  *) echo "usage: $0 {start|stop|status|reset}" >&2; exit 1 ;;
esac
