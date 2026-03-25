CREATE TABLE oms.claims (
    claim_id BIGINT PRIMARY KEY,
    order_id BIGINT,
    resource TEXT NOT NULL,
    resource_id BIGINT,
    status TEXT NOT NULL,
    claim_type TEXT NOT NULL,
    stage TEXT,
    parent_claim_id BIGINT,
    reason_id TEXT,
    fulfilled BOOLEAN,
    quantity_type TEXT,
    site_id TEXT,
    date_created TIMESTAMPTZ NOT NULL,
    last_updated TIMESTAMPTZ NOT NULL,
    raw_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_claims_order
        FOREIGN KEY (order_id)
        REFERENCES oms.orders (order_id)
);

CREATE INDEX idx_claims_order_id
    ON oms.claims (order_id);

CREATE INDEX idx_claims_status
    ON oms.claims (status);

CREATE INDEX idx_claims_last_updated
    ON oms.claims (last_updated);

CREATE INDEX idx_claims_resource_resource_id
    ON oms.claims (resource, resource_id);
