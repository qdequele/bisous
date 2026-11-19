-- Retention: `prune_sent_events` deletes delivered events by age.

CREATE INDEX IF NOT EXISTS lab_events_outbox_sent_idx
    ON lab_events_outbox (sent_at) WHERE sent_at IS NOT NULL;
