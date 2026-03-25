CREATE TABLE oms.order_operations (
    order_id BIGINT PRIMARY KEY,
    internal_status TEXT NOT NULL DEFAULT 'pending',
    picking_status TEXT,
    packing_status TEXT,
    invoice_status TEXT NOT NULL DEFAULT 'pending',
    invoice_number TEXT,
    invoice_issued_at TIMESTAMPTZ,
    remito_status TEXT NOT NULL DEFAULT 'pending',
    remito_number TEXT,
    remito_issued_at TIMESTAMPTZ,
    ready_to_ship_at TIMESTAMPTZ,
    delivered_internal_at TIMESTAMPTZ,
    notes TEXT,
    raw_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_order_operations_order
        FOREIGN KEY (order_id)
        REFERENCES oms.orders (order_id)
);

CREATE INDEX idx_order_operations_internal_status
    ON oms.order_operations (internal_status);

CREATE INDEX idx_order_operations_invoice_status
    ON oms.order_operations (invoice_status);

CREATE INDEX idx_order_operations_remito_status
    ON oms.order_operations (remito_status);
