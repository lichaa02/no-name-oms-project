CREATE TABLE oms.orders (
    order_id BIGINT PRIMARY KEY,
    site_id TEXT,
    pack_id BIGINT,
    status TEXT NOT NULL,
    status_detail TEXT,
    date_created TIMESTAMPTZ NOT NULL,
    last_updated TIMESTAMPTZ NOT NULL,
    total_amount NUMERIC(18,2),
    currency_id TEXT,
    buyer_id BIGINT,
    seller_id BIGINT,
    shipment_id BIGINT,
    raw_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orders_last_updated
    ON oms.orders (last_updated);

CREATE INDEX idx_orders_shipment_id
    ON oms.orders (shipment_id);

CREATE INDEX idx_orders_buyer_id
    ON oms.orders (buyer_id);

CREATE INDEX idx_orders_seller_id
    ON oms.orders (seller_id);
