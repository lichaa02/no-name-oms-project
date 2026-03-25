CREATE TABLE oms.sync_state (
    resource            TEXT        NOT NULL,
    account_id          BIGINT      NOT NULL,
    cursor_ts           TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00',
    overlap_seconds     INTEGER     NOT NULL DEFAULT 300
        CHECK (overlap_seconds BETWEEN 0 AND 3600),
    last_high_watermark TIMESTAMPTZ,
    last_attempt_at     TIMESTAMPTZ,
    last_success_at     TIMESTAMPTZ,
    status              TEXT        NOT NULL DEFAULT 'idle'
        CHECK (status IN ('idle', 'running', 'error')),
    last_error          TEXT,
    extra               JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (resource, account_id)
);

CREATE INDEX idx_sync_state_status
    ON oms.sync_state (status);
