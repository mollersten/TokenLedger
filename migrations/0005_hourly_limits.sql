-- Hourly request limits and the free plan.
--
-- `hourly_request_limit` caps requests per account per clock hour (UTC). The
-- first time an account goes over it, a one-time grace period starts
-- (`grace_ends_at`, 24 hours later) during which the cap is
-- `grace_hourly_request_limit` instead. Once the grace period is over the
-- plain hourly limit applies again and requests above it get 429s until the
-- account moves to a plan without the limit.
--
-- The free plan is 120 requests/hour, 600/hour during the grace period and
-- 10 requests/second; those defaults live in `accounts.rs`, and every limit
-- is stored per account so it can be overridden.

ALTER TABLE accounts DROP CONSTRAINT accounts_plan_check;
ALTER TABLE accounts ADD CONSTRAINT accounts_plan_check CHECK (plan IN ('free', 'trial', 'paid'));

ALTER TABLE accounts
    ADD COLUMN hourly_request_limit       INTEGER
        CHECK (hourly_request_limit IS NULL OR hourly_request_limit >= 0),
    ADD COLUMN grace_hourly_request_limit INTEGER
        CHECK (grace_hourly_request_limit IS NULL OR grace_hourly_request_limit >= 0),
    -- NULL until the account first goes over its hourly limit. Never reset
    -- automatically, so the grace period is used once.
    ADD COLUMN grace_ends_at              TIMESTAMPTZ;

-- Requests per account per clock hour, flushed from memory in batches like
-- `account_usage`. Kept for every account, limited or not.
CREATE TABLE account_hourly_usage (
    account_id     TEXT        NOT NULL REFERENCES accounts (id),
    hour_start     TIMESTAMPTZ NOT NULL,
    request_count  BIGINT      NOT NULL DEFAULT 0 CHECK (request_count >= 0),
    PRIMARY KEY (account_id, hour_start)
);
