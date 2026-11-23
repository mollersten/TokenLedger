-- Which credit buckets funded what, as rows instead of JSON arrays.
--
-- A reservation's hold, and a ledger entry's movement, can be split across
-- several expiring-credit buckets. That was stored as JSON arrays
-- (`reservations.bucket_holds`, `ledger_entries.metadata->'buckets'`); it is
-- a relation, so it gets a table each. A part's expiry is its bucket's, so
-- it is not repeated here.

CREATE TABLE reservations_credit_buckets (
    reservation_id  UUID   NOT NULL REFERENCES reservations (id),
    bucket_id       UUID   NOT NULL REFERENCES credit_buckets (id),
    amount          BIGINT NOT NULL CHECK (amount > 0),
    PRIMARY KEY (reservation_id, bucket_id)
);

CREATE TABLE ledger_entries_credit_buckets (
    ledger_entry_id  UUID   NOT NULL REFERENCES ledger_entries (id),
    bucket_id        UUID   NOT NULL REFERENCES credit_buckets (id),
    amount           BIGINT NOT NULL CHECK (amount > 0),
    PRIMARY KEY (ledger_entry_id, bucket_id)
);
-- Reconciling a bucket (granted = remaining + held + consumed + expired).
CREATE INDEX ledger_entries_credit_buckets_bucket_idx ON ledger_entries_credit_buckets (bucket_id);

-- Carry over what is already stored. Ledger entries are append-only, so
-- their old metadata keeps its copy; new entries no longer write one.
INSERT INTO reservations_credit_buckets (reservation_id, bucket_id, amount)
SELECT r.id, (h->>'bucket_id')::uuid, SUM((h->>'amount')::bigint)
FROM reservations r, jsonb_array_elements(r.bucket_holds) h
WHERE (h->>'amount')::bigint > 0
GROUP BY 1, 2;

INSERT INTO ledger_entries_credit_buckets (ledger_entry_id, bucket_id, amount)
SELECT l.id, (b->>'bucket_id')::uuid, SUM((b->>'amount')::bigint)
FROM ledger_entries l, jsonb_array_elements(l.metadata->'buckets') b
WHERE l.metadata ? 'buckets' AND (b->>'amount')::bigint > 0
GROUP BY 1, 2;

ALTER TABLE reservations DROP COLUMN bucket_holds;

ALTER TABLE reservations_credit_buckets ENABLE ROW LEVEL SECURITY;
ALTER TABLE ledger_entries_credit_buckets ENABLE ROW LEVEL SECURITY;
