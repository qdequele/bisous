-- Metering (docs/LAB_INTEGRATION.md, "Pricing"). Additive: old rows read
-- with the defaults.
--
-- Why the resync in progress runs (`user` or `system`), so a crash in the
-- middle of it does not change how the rest of it is metered.
ALTER TABLE sync_state ADD COLUMN IF NOT EXISTS resync_reason TEXT NULL;

-- The metered units, per sync and UTC day, next to the older counters.
ALTER TABLE sync_progress ADD COLUMN IF NOT EXISTS documents_streamed BIGINT NOT NULL DEFAULT 0;
ALTER TABLE sync_progress ADD COLUMN IF NOT EXISTS documents_snapshot BIGINT NOT NULL DEFAULT 0;
ALTER TABLE sync_progress ADD COLUMN IF NOT EXISTS documents_resync BIGINT NOT NULL DEFAULT 0;
ALTER TABLE sync_progress ADD COLUMN IF NOT EXISTS bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE sync_progress ADD COLUMN IF NOT EXISTS sync_seconds BIGINT NOT NULL DEFAULT 0;
