-- Moving customers over from another billing system (POST /v1/import).
--
-- Import facts about a wallet, kept off the wallets row (which is rewritten
-- on every request) and present only for wallets that were imported:
--   first_imported_at    when the tenant was first moved over (any import)
--   credits_imported_at  its starting balance can be imported once. Imports
--                        don't count toward plan limits, so without this an
--                        import could stand in for `grant` forever.
--   credits_import_key   the import (Idempotency-Key) that brought the
--                        balance in, so rerunning that same import later
--                        skips it instead of failing, even after its
--                        idempotency records have been pruned.
CREATE TABLE wallet_imports (
    wallet_id            BIGINT      PRIMARY KEY REFERENCES wallets (id),
    first_imported_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    credits_imported_at  TIMESTAMPTZ,
    credits_import_key   TEXT,
    CHECK ((credits_imported_at IS NULL) = (credits_import_key IS NULL))
);

-- Safety ceilings on imports, per account and UTC day, counted separately:
--   customers  tenants imported for the first time
--   purchases  wallets_items rows actually written
-- They are not plan limits (an account can move over as many customers as
-- it likes); they only stop a runaway or malicious client from writing
-- millions of rows.
CREATE TABLE account_import_usage (
    account_id  TEXT   NOT NULL REFERENCES accounts (id),
    day         DATE   NOT NULL,
    customers   BIGINT NOT NULL DEFAULT 0 CHECK (customers >= 0),
    purchases   BIGINT NOT NULL DEFAULT 0 CHECK (purchases >= 0),
    PRIMARY KEY (account_id, day)
);
