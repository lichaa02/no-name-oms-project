SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'oms'
  AND table_name = 'orders';

SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'oms'
  AND tablename = 'orders';
