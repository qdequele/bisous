-- Bounded growth. Additive: existing rows get the defaults.
--
-- Outbox: an event the Lab refused or could not take waits longer after
-- each failed delivery (`attempts`, `next_attempt_at`) instead of coming
-- back every lease.
ALTER TABLE lab_events_outbox ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE lab_events_outbox ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NULL;

-- Dead letters: past the per-sync cap rejected documents are counted here
-- instead of stored.
ALTER TABLE sync_progress ADD COLUMN IF NOT EXISTS dead_letters_dropped BIGINT NOT NULL DEFAULT 0;
