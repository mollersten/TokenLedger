-- TokenLedger: usage metering, credit ledger & entitlements.
--
-- Lock order used by every writer (API handlers and the TTL reaper):
--   1. wallets row (FOR UPDATE)   2. reservations row (FOR UPDATE)
-- The reaper never *waits* for a wallet lock (it uses SKIP LOCKED), so it
-- cannot deadlock with request handlers even though it selects reservations
-- first.

-- ---------------------------------------------------------------------------
-- Wallets: one row per tenant, the unit of pessimistic locking.
-- ---------------------------------------------------------------------------
CREATE TABLE wallets (
    tenant_id          TEXT        PRIMARY KEY,
    available_credits  BIGINT      NOT NULL DEFAULT 0 CHECK (available_credits >= 0),
    reserved_credits   BIGINT      NOT NULL DEFAULT 0 CHECK (reserved_credits >= 0),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Reservations: credit holds that must be committed, released or expired.
-- ---------------------------------------------------------------------------
CREATE TABLE reservations (
    id                UUID        PRIMARY KEY,
    tenant_id         TEXT        NOT NULL REFERENCES wallets (tenant_id),
    feature           TEXT        NOT NULL,
    reserved_amount   BIGINT      NOT NULL CHECK (reserved_amount > 0),
    committed_amount  BIGINT               CHECK (committed_amount IS NULL OR committed_amount >= 0),
    status            TEXT        NOT NULL CHECK (status IN ('PENDING', 'COMMITTED', 'RELEASED', 'EXPIRED')),
    expires_at        TIMESTAMPTZ NOT NULL,
    metadata          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Fast reaper polling: only PENDING rows are indexed.
CREATE INDEX reservations_pending_expiry_idx
    ON reservations (status, expires_at)
    WHERE status = 'PENDING';

-- ---------------------------------------------------------------------------
-- Ledger entries: append-only audit log of every balance change.
-- ---------------------------------------------------------------------------
CREATE TABLE ledger_entries (
    id                       UUID        PRIMARY KEY,
    tenant_id                TEXT        NOT NULL REFERENCES wallets (tenant_id),
    reservation_id           UUID                 REFERENCES reservations (id),
    entry_type               TEXT        NOT NULL CHECK (entry_type IN (
                                 'GRANT', 'RESERVE', 'COMMIT', 'REFUND_UNUSED',
                                 'OVERAGE_DEDUCT', 'RELEASE', 'EXPIRE')),
    delta_available          BIGINT      NOT NULL,
    delta_reserved           BIGINT      NOT NULL,
    balance_available_after  BIGINT      NOT NULL CHECK (balance_available_after >= 0),
    balance_reserved_after   BIGINT      NOT NULL CHECK (balance_reserved_after >= 0),
    metadata                 JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX ledger_entries_tenant_created_idx
    ON ledger_entries (tenant_id, created_at DESC);

-- Enforce immutability at the database level, not just in application code.
CREATE FUNCTION ledger_entries_reject_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'ledger_entries is append-only: % is not allowed', TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$$;

CREATE TRIGGER ledger_entries_no_update_delete
    BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_entries_reject_mutation();

CREATE TRIGGER ledger_entries_no_truncate
    BEFORE TRUNCATE ON ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_entries_reject_mutation();

-- ---------------------------------------------------------------------------
-- Idempotency keys: cached responses for replayed mutating requests.
-- ---------------------------------------------------------------------------
CREATE TABLE idempotency_keys (
    tenant_id        TEXT        NOT NULL,
    idempotency_key  TEXT        NOT NULL,
    endpoint         TEXT        NOT NULL,
    request_hash     TEXT        NOT NULL,
    response_status  SMALLINT    NOT NULL,
    response_body    JSONB       NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, idempotency_key)
);
