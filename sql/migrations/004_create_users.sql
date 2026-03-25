CREATE TABLE oms.users (
    user_id BIGINT PRIMARY KEY,
    nickname TEXT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone TEXT,
    alternative_phone TEXT,
    site_id TEXT,
    raw_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_users_nickname
    ON oms.users (nickname);

ALTER TABLE oms.orders
    ADD CONSTRAINT fk_orders_buyer
    FOREIGN KEY (buyer_id)
    REFERENCES oms.users (user_id);

ALTER TABLE oms.orders
    ADD CONSTRAINT fk_orders_seller
    FOREIGN KEY (seller_id)
    REFERENCES oms.users (user_id);
