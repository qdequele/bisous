-- The live indexes a snapshot in progress fills from empty
-- (`SyncState::filling`): an interrupted first copy empties them and fills
-- them again in place, instead of rebuilding them in a shadow index.
-- Additive: old rows read as "none". A process of the previous version still
-- running during a deploy never sets the column and leaves it as it is.
ALTER TABLE sync_state ADD COLUMN IF NOT EXISTS filling TEXT[] NOT NULL DEFAULT '{}';
