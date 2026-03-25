SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'oms'
  AND table_name = 'order_operations';

SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'oms'
  AND tablename = 'order_operations';

SELECT conname, contype
FROM pg_constraint
WHERE conrelid = 'oms.order_operations'::regclass
ORDER BY conname;
