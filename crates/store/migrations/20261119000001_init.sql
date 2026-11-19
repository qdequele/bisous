-- Initial schema. Migrations are forward-only and additive: never edit an
-- applied file, add a new one.

CREATE TABLE IF NOT EXISTS syncs (
    id          TEXT PRIMARY KEY,
    tenant_id   TEXT NULL,
    name        TEXT NOT NULL,
    kind        TEXT NOT NULL,
    -- The whole SyncSpec as JSON, sealed: version(1) || nonce(12) || ciphertext.
    spec_sealed BYTEA NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS syncs_name_tenant_key
    ON syncs (name, coalesce(tenant_id, ''));
CREATE INDEX IF NOT EXISTS syncs_tenant_idx ON syncs (tenant_id);

CREATE TABLE IF NOT EXISTS sync_state (
    sync_id    TEXT PRIMARY KEY REFERENCES syncs (id) ON DELETE CASCADE,
    phase      TEXT NOT NULL,
    checkpoint JSONB NULL,
    message    TEXT NULL,
    resyncing  BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS dead_letters (
    id          BIGSERIAL PRIMARY KEY,
    sync_id     TEXT NOT NULL REFERENCES syncs (id) ON DELETE CASCADE,
    index_uid   TEXT NOT NULL,
    document_id TEXT NOT NULL,
    payload     JSONB NOT NULL,
    reason      TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dead_letters_sync_idx ON dead_letters (sync_id, id);

CREATE TABLE IF NOT EXISTS sync_progress (
    sync_id  TEXT NOT NULL REFERENCES syncs (id) ON DELETE CASCADE,
    day      DATE NOT NULL,
    written  BIGINT NOT NULL DEFAULT 0,
    deleted  BIGINT NOT NULL DEFAULT 0,
    rejected BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (sync_id, day)
);

CREATE TABLE IF NOT EXISTS lab_events_outbox (
    id           UUID PRIMARY KEY,
    tenant_id    TEXT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    leased_until TIMESTAMPTZ NULL,
    sent_at      TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS lab_events_outbox_unsent_idx
    ON lab_events_outbox (created_at, id) WHERE sent_at IS NULL;
