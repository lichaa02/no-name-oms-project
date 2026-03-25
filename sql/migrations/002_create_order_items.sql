CREATE TABLE oms.order_items (
    order_item_row_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES oms.orders(order_id),
    item_id TEXT NOT NULL,
    variation_id BIGINT,
    variation_key BIGINT GENERATED ALWAYS AS (COALESCE(variation_id, -1)) STORED,
    title TEXT,
    category_id TEXT,
    seller_sku TEXT,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(18,2) NOT NULL CHECK (unit_price >= 0),
    full_unit_price NUMERIC(18,2),
    sale_fee NUMERIC(18,2),
    listing_type_id TEXT,
    raw_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_order_items_order_variation
        UNIQUE (order_id, item_id, variation_key)
);

CREATE INDEX idx_order_items_item_id
    ON oms.order_items (item_id);
