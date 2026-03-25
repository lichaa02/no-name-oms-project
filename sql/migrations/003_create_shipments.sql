CREATE TABLE oms.shipments (
    shipment_id BIGINT PRIMARY KEY,
    site_id TEXT,
    mode TEXT,
    logistic_type TEXT,
    status TEXT NOT NULL,
    substatus TEXT,
    tracking_number TEXT,
    tracking_method TEXT,
    shipping_option_id BIGINT,
    shipping_option_name TEXT,
    date_created TIMESTAMPTZ,
    last_updated TIMESTAMPTZ,
    raw_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_shipments_status
    ON oms.shipments (status);

CREATE INDEX idx_shipments_last_updated
    ON oms.shipments (last_updated);

ALTER TABLE oms.orders
    ADD CONSTRAINT fk_orders_shipment
    FOREIGN KEY (shipment_id)
    REFERENCES oms.shipments (shipment_id);
