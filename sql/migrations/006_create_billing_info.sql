CREATE TABLE oms.billing_info (
    order_id BIGINT PRIMARY KEY,
    doc_type TEXT,
    doc_number TEXT,
    taxpayer_type TEXT,
    billing_name TEXT,
    address_line TEXT,
    street_name TEXT,
    street_number TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    zip_code TEXT,
    raw_json JSONB NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_billing_info_order
        FOREIGN KEY (order_id)
        REFERENCES oms.orders (order_id)
);

CREATE INDEX idx_billing_info_doc_number
    ON oms.billing_info (doc_number);

CREATE INDEX idx_billing_info_taxpayer_type
    ON oms.billing_info (taxpayer_type);
