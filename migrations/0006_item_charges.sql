-- Charge once per item.
--
-- An item is something a tenant buys once and then owns, e.g. the email
-- address of person 42 under the feature `email_lookup`. Items are stored
-- once per account, however many tenants buy them; who owns what is a row
-- of two numbers in `wallets_items`. A tenant never pays twice for an item it
-- owns, even when its own code crashes after getting it and asks again.

-- A compact numeric id for each wallet (tenant), used as a key below.
ALTER TABLE wallets ADD COLUMN id BIGINT GENERATED ALWAYS AS IDENTITY;
ALTER TABLE wallets ADD CONSTRAINT wallets_id_key UNIQUE (id);

-- The feature is part of an item's identity: the same key under another
-- feature is a different item, bought separately.
CREATE TABLE items (
    id          BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id  TEXT        NOT NULL REFERENCES accounts (id),
    feature     TEXT        NOT NULL,
    item_key    TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, feature, item_key)
);

-- A row means the wallet owns the item: it was paid for, by a charge
-- (`reservation_id`) or in the system the account moved from (NULL).
CREATE TABLE wallets_items (
    wallet_id       BIGINT      NOT NULL REFERENCES wallets (id),
    item_id         BIGINT      NOT NULL REFERENCES items (id),
    credits_paid    BIGINT      NOT NULL CHECK (credits_paid > 0),
    reservation_id  UUID                 REFERENCES reservations (id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (wallet_id, item_id)
);

-- A charge in progress is a PENDING reservation for the item. At most one
-- per tenant and item; when it commits, its wallets_items row is written in
-- the same transaction. Request handlers enforce both under the wallet lock;
-- this index and the primary key above are the backstop.
ALTER TABLE reservations ADD COLUMN item_id BIGINT REFERENCES items (id);
CREATE UNIQUE INDEX reservations_pending_item_idx
    ON reservations (account_id, tenant_id, item_id)
    WHERE item_id IS NOT NULL AND status = 'PENDING';

-- An account can let owned items expire (`item_retention_days`, set through
-- PATCH /v1/settings; NULL keeps them forever). An item bought longer ago
-- than that is no longer owned, so the next request for it is charged again,
-- and the server deletes such rows in the background (this index finds them).
ALTER TABLE accounts ADD COLUMN item_retention_days INTEGER
    CHECK (item_retention_days IS NULL OR item_retention_days > 0);
CREATE INDEX wallets_items_created_at_idx ON wallets_items (created_at);
