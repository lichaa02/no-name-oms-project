BEGIN;

CREATE SCHEMA IF NOT EXISTS app;

ALTER TABLE IF EXISTS oms.users RENAME TO meli_users;
ALTER TABLE IF EXISTS oms.shipments RENAME TO meli_shipments;
ALTER TABLE IF EXISTS oms.orders RENAME TO meli_orders;
ALTER TABLE IF EXISTS oms.order_items RENAME TO meli_order_items;
ALTER TABLE IF EXISTS oms.claims RENAME TO meli_claims;
ALTER TABLE IF EXISTS oms.billing_info RENAME TO meli_billing_info;
ALTER TABLE IF EXISTS oms.order_operations RENAME TO meli_order_operations;
ALTER TABLE IF EXISTS oms.sync_state RENAME TO meli_sync_state;

ALTER SEQUENCE IF EXISTS oms.order_items_order_item_row_id_seq
    RENAME TO meli_order_items_order_item_row_id_seq;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'oms'
          AND t.relname = 'meli_users'
          AND c.conname = 'users_pkey'
    ) THEN
        EXECUTE 'ALTER TABLE oms.meli_users RENAME CONSTRAINT users_pkey TO meli_users_pkey';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'oms'
          AND t.relname = 'meli_shipments'
          AND c.conname = 'shipments_pkey'
    ) THEN
        EXECUTE 'ALTER TABLE oms.meli_shipments RENAME CONSTRAINT shipments_pkey TO meli_shipments_pkey';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'oms'
          AND t.relname = 'meli_orders'
          AND c.conname = 'orders_pkey'
    ) THEN
        EXECUTE 'ALTER TABLE oms.meli_orders RENAME CONSTRAINT orders_pkey TO meli_orders_pkey';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'oms'
          AND t.relname = 'meli_orders'
          AND c.conname = 'fk_orders_shipment'
    ) THEN
        EXECUTE 'ALTER TABLE oms.meli_orders RENAME CONSTRAINT fk_orders_shipment TO fk_meli_orders_shipment';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'oms'
          AND t.relname = 'meli_orders'
          AND c.conname = 'fk_orders_buyer'
    ) THEN
        EXECUTE 'ALTER TABLE oms.meli_orders RENAME CONSTRAINT fk_orders_buyer TO fk_meli_orders_buyer';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'oms'
          AND t.relname = 'meli_orders'
          AND c.conname = 'fk_orders_seller'
    ) THEN
        EXECUTE 'ALTER TABLE oms.meli_orders RENAME CONSTRAINT fk_orders_seller TO fk_meli_orders_seller';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'oms'
          AND t.relname = 'meli_order_items'
          AND c.conname = 'order_items_pkey'
    ) THEN
        EXECUTE 'ALTER TABLE oms.meli_order_items RENAME CONSTRAINT order_items_pkey TO meli_order_items_pkey';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'oms'
          AND t.relname = 'meli_order_items'
          AND c.conname = 'order_items_order_id_fkey'
    ) THEN
        EXECUTE 'ALTER TABLE oms.meli_order_items RENAME CONSTRAINT order_items_order_id_fkey TO meli_order_items_order_id_fkey';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'oms'
          AND t.relname = 'meli_order_items'
          AND c.conname = 'uq_order_items_order_variation'
    ) THEN
        EXECUTE 'ALTER TABLE oms.meli_order_items RENAME CONSTRAINT uq_order_items_order_variation TO uq_meli_order_items_order_variation';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'oms'
          AND t.relname = 'meli_claims'
          AND c.conname = 'claims_pkey'
    ) THEN
        EXECUTE 'ALTER TABLE oms.meli_claims RENAME CONSTRAINT claims_pkey TO meli_claims_pkey';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'oms'
          AND t.relname = 'meli_claims'
          AND c.conname = 'fk_claims_order'
    ) THEN
        EXECUTE 'ALTER TABLE oms.meli_claims RENAME CONSTRAINT fk_claims_order TO fk_meli_claims_order';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'oms'
          AND t.relname = 'meli_billing_info'
          AND c.conname = 'billing_info_pkey'
    ) THEN
        EXECUTE 'ALTER TABLE oms.meli_billing_info RENAME CONSTRAINT billing_info_pkey TO meli_billing_info_pkey';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'oms'
          AND t.relname = 'meli_billing_info'
          AND c.conname = 'fk_billing_info_order'
    ) THEN
        EXECUTE 'ALTER TABLE oms.meli_billing_info RENAME CONSTRAINT fk_billing_info_order TO fk_meli_billing_info_order';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'oms'
          AND t.relname = 'meli_order_operations'
          AND c.conname = 'order_operations_pkey'
    ) THEN
        EXECUTE 'ALTER TABLE oms.meli_order_operations RENAME CONSTRAINT order_operations_pkey TO meli_order_operations_pkey';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'oms'
          AND t.relname = 'meli_order_operations'
          AND c.conname = 'fk_order_operations_order'
    ) THEN
        EXECUTE 'ALTER TABLE oms.meli_order_operations RENAME CONSTRAINT fk_order_operations_order TO fk_meli_order_operations_order';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'oms'
          AND t.relname = 'meli_sync_state'
          AND c.conname = 'sync_state_pkey'
    ) THEN
        EXECUTE 'ALTER TABLE oms.meli_sync_state RENAME CONSTRAINT sync_state_pkey TO meli_sync_state_pkey';
    END IF;
END
$$;

DO $$
BEGIN
    IF to_regclass('oms.idx_users_nickname') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_users_nickname RENAME TO idx_meli_users_nickname';
    END IF;

    IF to_regclass('oms.idx_shipments_status') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_shipments_status RENAME TO idx_meli_shipments_status';
    END IF;

    IF to_regclass('oms.idx_shipments_last_updated') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_shipments_last_updated RENAME TO idx_meli_shipments_last_updated';
    END IF;

    IF to_regclass('oms.idx_orders_last_updated') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_orders_last_updated RENAME TO idx_meli_orders_last_updated';
    END IF;

    IF to_regclass('oms.idx_orders_shipment_id') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_orders_shipment_id RENAME TO idx_meli_orders_shipment_id';
    END IF;

    IF to_regclass('oms.idx_orders_buyer_id') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_orders_buyer_id RENAME TO idx_meli_orders_buyer_id';
    END IF;

    IF to_regclass('oms.idx_orders_seller_id') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_orders_seller_id RENAME TO idx_meli_orders_seller_id';
    END IF;

    IF to_regclass('oms.idx_order_items_item_id') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_order_items_item_id RENAME TO idx_meli_order_items_item_id';
    END IF;

    IF to_regclass('oms.idx_claims_order_id') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_claims_order_id RENAME TO idx_meli_claims_order_id';
    END IF;

    IF to_regclass('oms.idx_claims_status') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_claims_status RENAME TO idx_meli_claims_status';
    END IF;

    IF to_regclass('oms.idx_claims_last_updated') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_claims_last_updated RENAME TO idx_meli_claims_last_updated';
    END IF;

    IF to_regclass('oms.idx_claims_resource_resource_id') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_claims_resource_resource_id RENAME TO idx_meli_claims_resource_resource_id';
    END IF;

    IF to_regclass('oms.idx_billing_info_doc_number') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_billing_info_doc_number RENAME TO idx_meli_billing_info_doc_number';
    END IF;

    IF to_regclass('oms.idx_billing_info_taxpayer_type') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_billing_info_taxpayer_type RENAME TO idx_meli_billing_info_taxpayer_type';
    END IF;

    IF to_regclass('oms.idx_order_operations_internal_status') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_order_operations_internal_status RENAME TO idx_meli_order_operations_internal_status';
    END IF;

    IF to_regclass('oms.idx_order_operations_invoice_status') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_order_operations_invoice_status RENAME TO idx_meli_order_operations_invoice_status';
    END IF;

    IF to_regclass('oms.idx_order_operations_remito_status') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_order_operations_remito_status RENAME TO idx_meli_order_operations_remito_status';
    END IF;

    IF to_regclass('oms.idx_sync_state_status') IS NOT NULL THEN
        EXECUTE 'ALTER INDEX oms.idx_sync_state_status RENAME TO idx_meli_sync_state_status';
    END IF;
END
$$;

COMMIT;
