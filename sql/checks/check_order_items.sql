SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'oms'
  AND table_name = 'order_items';

SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'oms'
  AND tablename = 'order_items';

SELECT conname, contype
FROM pg_constraint
WHERE conrelid = 'oms.order_items'::regclass;
