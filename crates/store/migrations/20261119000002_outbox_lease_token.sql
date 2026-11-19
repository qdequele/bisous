-- One token per lease: only the leaser that still holds an event may mark
-- it sent, so a worker whose lease expired (and was taken over) cannot mark
-- the other worker's delivery. Additive: NULL for rows leased before.

ALTER TABLE lab_events_outbox ADD COLUMN IF NOT EXISTS lease_token UUID NULL;
