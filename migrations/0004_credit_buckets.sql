-- Expiring credits.
--
-- A grant with `expires_at` puts its credits in a bucket instead of the
-- wallet's permanent balance. Spending always draws from buckets first,
-- soonest expiry first, then from permanent credits. At `expires_at` whatever
-- is left in the bucket is removed with a CREDIT_EXPIRE ledger entry.
--
-- That covers monthly allowances that reset (one bucket per period), plan
-- credits that expire next to top-ups that never do, rollover windows and
-- daily caps.
--
-- Buckets, like reservations, are only modified while their wallet row is
-- locked, so the existing lock order (wallet, then reservation) is unchanged.

CREATE TABLE credit_buckets (
    id          UUID        PRIMARY KEY,
    account_id  TEXT        NOT NULL,
    tenant_id   TEXT        NOT NULL,
    granted     BIGINT      NOT NULL CHECK (granted > 0),
    remaining   BIGINT      NOT NULL CHECK (remaining >= 0 AND remaining <= granted),
    expires_at  TIMESTAMPTZ NOT NULL,
    reason      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Set once the bucket's expiry has been processed; it is then final.
    settled_at  TIMESTAMPTZ,
    FOREIGN KEY (account_id, tenant_id) REFERENCES wallets (account_id, tenant_id)
) WITH (fillfactor = 80);

-- Spend order within a wallet, and the reaper's scan for due buckets.
-- Every reserve and refund updates `remaining`, so no index mentions it,
-- not even in a predicate: those updates stay HOT (no new index entries),
-- which matters when one tenant spends from the same bucket thousands of
-- times a second. The indexes only shrink when a bucket settles.
CREATE INDEX credit_buckets_wallet_live_idx
    ON credit_buckets (account_id, tenant_id, expires_at, id)
    WHERE settled_at IS NULL;
CREATE INDEX credit_buckets_due_idx
    ON credit_buckets (expires_at)
    WHERE settled_at IS NULL;

-- `expiring_credits` is the part of `available_credits` held in buckets
-- (the sum of their `remaining`); the rest is permanent. `next_expiry_at` is
-- the earliest `expires_at` among buckets with credits left, so a request
-- only reads buckets when one is due or when it has to spend from them.
ALTER TABLE wallets
    ADD COLUMN expiring_credits BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN next_expiry_at   TIMESTAMPTZ,
    ADD CONSTRAINT wallets_expiring_credits_check
        CHECK (expiring_credits >= 0 AND expiring_credits <= available_credits);

-- Which buckets funded each hold: [{"bucket_id", "amount", "expires_at"}].
-- Unused credits go back to the same buckets; a part whose bucket expired in
-- the meantime expires on return.
ALTER TABLE reservations
    ADD COLUMN bucket_holds JSONB NOT NULL DEFAULT '[]'::jsonb;

-- GRANT entries that created a bucket and CREDIT_EXPIRE entries point at it.
-- (Adding a nullable column and swapping a CHECK are DDL, so the
-- append-only triggers are not involved.)
ALTER TABLE ledger_entries
    ADD COLUMN bucket_id UUID REFERENCES credit_buckets (id);

ALTER TABLE ledger_entries DROP CONSTRAINT ledger_entries_entry_type_check;
ALTER TABLE ledger_entries ADD CONSTRAINT ledger_entries_entry_type_check CHECK (entry_type IN (
    'GRANT', 'RESERVE', 'COMMIT', 'REFUND_UNUSED',
    'OVERAGE_DEDUCT', 'RELEASE', 'EXPIRE', 'CREDIT_EXPIRE'));
