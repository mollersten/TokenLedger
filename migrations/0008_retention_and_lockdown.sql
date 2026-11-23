-- 1. Idempotency records are kept for a limited time (24 hours by default,
--    IDEMPOTENCY_RETENTION_HOURS) and pruned by the server; this index lets
--    it find old rows without scanning the table. Charging once per item does
--    not depend on them (wallets_items does), so pruning never lets an item
--    be charged twice.
CREATE INDEX idempotency_keys_created_at_idx ON idempotency_keys (created_at);

-- 2. Keep every table away from the Supabase Data API.
--
-- Supabase serves tables in `public` over PostgREST to the `anon` and
-- `authenticated` roles. The server connects as the tables' owner, which
-- row level security does not restrict, so enabling it with no policies
-- shuts those roles out without affecting TokenLedger. Supabase may already
-- do this automatically; this makes it explicit. The revokes only run where
-- those roles exist (not on a plain Postgres).
DO $$
DECLARE
    t TEXT;
BEGIN
    FOR t IN SELECT tablename FROM pg_tables WHERE schemaname = 'public' LOOP
        EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', t);
    END LOOP;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
        EXECUTE 'REVOKE ALL ON ALL TABLES IN SCHEMA public FROM anon';
        EXECUTE 'REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM anon';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
        EXECUTE 'REVOKE ALL ON ALL TABLES IN SCHEMA public FROM authenticated';
        EXECUTE 'REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM authenticated';
    END IF;
END
$$;
