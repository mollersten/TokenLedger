-- Accounts (your SaaS customers), API keys, monthly usage, and per-account
-- isolation: every wallet, reservation, ledger entry and idempotency key now
-- belongs to exactly one account, and `cus_123` at two different accounts
-- are two unrelated wallets.

CREATE TABLE accounts (
    id                     TEXT        PRIMARY KEY,
    name                   TEXT        NOT NULL,
    plan                   TEXT        NOT NULL DEFAULT 'trial' CHECK (plan IN ('trial', 'paid')),
    status                 TEXT        NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended')),
    trial_ends_at          TIMESTAMPTZ,
    -- NULL limit = unlimited.
    monthly_request_limit  BIGINT      CHECK (monthly_request_limit IS NULL OR monthly_request_limit >= 0),
    max_tenants            INTEGER     CHECK (max_tenants IS NULL OR max_tenants >= 0),
    rate_limit_per_sec     INTEGER     CHECK (rate_limit_per_sec IS NULL OR rate_limit_per_sec > 0),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Only a SHA-256 hash of each key is stored; the plaintext is shown once.
CREATE TABLE api_keys (
    id          TEXT        PRIMARY KEY,
    account_id  TEXT        NOT NULL REFERENCES accounts (id),
    key_prefix  TEXT        NOT NULL,
    key_hash    TEXT        NOT NULL UNIQUE,
    label       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at  TIMESTAMPTZ
);

CREATE INDEX api_keys_account_idx ON api_keys (account_id);

-- Requests per account per calendar month (UTC), flushed from memory in batches.
CREATE TABLE account_usage (
    account_id     TEXT   NOT NULL REFERENCES accounts (id),
    period_start   DATE   NOT NULL,
    request_count  BIGINT NOT NULL DEFAULT 0 CHECK (request_count >= 0),
    PRIMARY KEY (account_id, period_start)
);

-- Rows written before authentication existed (local development only) are
-- attached to a suspended placeholder account. Adding the column with a
-- default fills existing rows without UPDATEs, so the append-only ledger's
-- triggers are untouched. On an empty database no placeholder is created.
INSERT INTO accounts (id, name, plan, status)
SELECT 'acct_preauth', 'Data created before authentication', 'paid', 'suspended'
WHERE EXISTS (SELECT 1 FROM wallets) OR EXISTS (SELECT 1 FROM idempotency_keys);

ALTER TABLE wallets          ADD COLUMN account_id TEXT NOT NULL DEFAULT 'acct_preauth';
ALTER TABLE reservations     ADD COLUMN account_id TEXT NOT NULL DEFAULT 'acct_preauth';
ALTER TABLE ledger_entries   ADD COLUMN account_id TEXT NOT NULL DEFAULT 'acct_preauth';
ALTER TABLE idempotency_keys ADD COLUMN account_id TEXT NOT NULL DEFAULT 'acct_preauth';

ALTER TABLE wallets          ALTER COLUMN account_id DROP DEFAULT;
ALTER TABLE reservations     ALTER COLUMN account_id DROP DEFAULT;
ALTER TABLE ledger_entries   ALTER COLUMN account_id DROP DEFAULT;
ALTER TABLE idempotency_keys ALTER COLUMN account_id DROP DEFAULT;

-- Re-key everything on (account_id, tenant_id).
ALTER TABLE reservations   DROP CONSTRAINT reservations_tenant_id_fkey;
ALTER TABLE ledger_entries DROP CONSTRAINT ledger_entries_tenant_id_fkey;
ALTER TABLE wallets        DROP CONSTRAINT wallets_pkey;

ALTER TABLE wallets ADD PRIMARY KEY (account_id, tenant_id);
ALTER TABLE wallets ADD CONSTRAINT wallets_account_id_fkey
    FOREIGN KEY (account_id) REFERENCES accounts (id);
ALTER TABLE reservations ADD CONSTRAINT reservations_wallet_fkey
    FOREIGN KEY (account_id, tenant_id) REFERENCES wallets (account_id, tenant_id);
ALTER TABLE ledger_entries ADD CONSTRAINT ledger_entries_wallet_fkey
    FOREIGN KEY (account_id, tenant_id) REFERENCES wallets (account_id, tenant_id);

ALTER TABLE idempotency_keys DROP CONSTRAINT idempotency_keys_pkey;
ALTER TABLE idempotency_keys ADD PRIMARY KEY (account_id, tenant_id, idempotency_key);
ALTER TABLE idempotency_keys ADD CONSTRAINT idempotency_keys_account_id_fkey
    FOREIGN KEY (account_id) REFERENCES accounts (id);

DROP INDEX ledger_entries_tenant_created_idx;
CREATE INDEX ledger_entries_wallet_created_idx
    ON ledger_entries (account_id, tenant_id, created_at DESC);
