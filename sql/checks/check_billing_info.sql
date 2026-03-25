SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'oms'
  AND table_name = 'billing_info';

SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'oms'
  AND tablename = 'billing_info';

SELECT conname, contype
FROM pg_constraint
WHERE conrelid = 'oms.billing_info'::regclass
ORDER BY conname;
