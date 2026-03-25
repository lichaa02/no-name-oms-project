SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'oms'
  AND table_name = 'shipments';

SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'oms'
  AND tablename = 'shipments';

SELECT conname, contype
FROM pg_constraint
WHERE conrelid = 'oms.shipments'::regclass
   OR conrelid = 'oms.orders'::regclass
ORDER BY conrelid::regclass::text, conname;
