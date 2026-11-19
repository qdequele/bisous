-- Durable usage (docs/LAB_INTEGRATION.md, "Usage event"). Additive.
--
-- Usage recorded with progress and not yet turned into an event, per lane
-- (`lab`, `tinybird`) and sync. `record_progress` adds to it in the same
-- transaction as `sync_progress`; the drain moves it into the outbox in one
-- transaction. No foreign key: the usage of a deleted sync is still sent.
CREATE TABLE IF NOT EXISTS usage_pending (
    lane               TEXT NOT NULL,
    sync_id            TEXT NOT NULL,
    tenant_id          TEXT NULL,
    source_kind        TEXT NOT NULL DEFAULT '',
    sync_name          TEXT NOT NULL DEFAULT '',
    documents_streamed BIGINT NOT NULL DEFAULT 0,
    documents_snapshot BIGINT NOT NULL DEFAULT 0,
    documents_resync   BIGINT NOT NULL DEFAULT 0,
    bytes              BIGINT NOT NULL DEFAULT 0,
    sync_seconds       BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (lane, sync_id)
);

-- One outbox, one lane per consumer, each leased and backed off on its own.
-- Rows written before this migration are the Lab's.
ALTER TABLE lab_events_outbox ADD COLUMN IF NOT EXISTS lane TEXT NOT NULL DEFAULT 'lab';

CREATE INDEX IF NOT EXISTS lab_events_outbox_lane_unsent_idx
    ON lab_events_outbox (lane, created_at, id) WHERE sent_at IS NULL;
