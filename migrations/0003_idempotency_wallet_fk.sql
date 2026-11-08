-- Every write transaction inserts an idempotency row. With a foreign key from
-- idempotency_keys to accounts, each insert took a KEY SHARE lock on the
-- account's single row, so every concurrent request of one account piled onto
-- that row (MultiXact contention). Reference the wallet instead: the writing
-- transaction already holds that row FOR UPDATE, so the check takes no
-- additional lock, and account integrity still follows via wallets.
ALTER TABLE idempotency_keys DROP CONSTRAINT idempotency_keys_account_id_fkey;
ALTER TABLE idempotency_keys ADD CONSTRAINT idempotency_keys_wallet_fkey
    FOREIGN KEY (account_id, tenant_id) REFERENCES wallets (account_id, tenant_id);
