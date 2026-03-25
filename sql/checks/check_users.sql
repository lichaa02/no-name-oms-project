SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'oms'
  AND table_name = 'users';

SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'oms'
  AND tablename = 'users';

SELECT conname, contype
FROM pg_constraint
WHERE conrelid = 'oms.users'::regclass
   OR conrelid = 'oms.orders'::regclass
ORDER BY conrelid::regclass::text, conname;
