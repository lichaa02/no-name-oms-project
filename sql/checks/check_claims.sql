SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'oms'
  AND table_name = 'claims';

SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'oms'
  AND tablename = 'claims';

SELECT conname, contype
FROM pg_constraint
WHERE conrelid = 'oms.claims'::regclass
ORDER BY conname;
