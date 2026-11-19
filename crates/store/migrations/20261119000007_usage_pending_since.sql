-- Idle usage (docs/LAB_INTEGRATION.md, "Usage event"). Additive.
--
-- When the first usage of a pending row was recorded, i.e. after the sync's
-- previous event: a row holding only `sync_seconds` is enqueued once it is
-- `USAGE_IDLE_FLUSH_SECS` old. Kept by the upsert, so it survives restarts.
-- Rows pending before this migration count from now: their event comes at
-- worst one idle period late, nothing is lost.
ALTER TABLE usage_pending
    ADD COLUMN IF NOT EXISTS pending_since TIMESTAMPTZ NOT NULL DEFAULT now();
