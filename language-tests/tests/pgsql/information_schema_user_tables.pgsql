/**
[env]
namespace = true
database = true
imports = ["pgsql/_catalog_listing.surql"]

[test]
reason = "information_schema.tables, information_schema.columns and pg_class list the tables, their primary key indexes and top-level fields of the selected database (an array column is `ARRAY`): id first, nested fields skipped, a schemaless table as its id alone"

[[test.results]]
value = "[{ table_name: 'catalog_note', table_schema: 'public' }, { table_name: 'catalog_user', table_schema: 'public' }]"

[[test.results]]
value = "[{ column_name: 'id', data_type: 'bigint', is_nullable: 'NO', ordinal_position: 1 }, { column_name: 'address', data_type: 'jsonb', is_nullable: 'NO', ordinal_position: 2 }, { column_name: 'name', data_type: 'text', is_nullable: 'NO', ordinal_position: 3 }, { column_name: 'tags', data_type: 'ARRAY', is_nullable: 'YES', ordinal_position: 4 }]"

[[test.results]]
value = "[{ column_name: 'id', table_name: 'catalog_note' }]"

[[test.results]]
value = "[{ relname: 'catalog_note' }, { relname: 'catalog_note_pkey' }, { relname: 'catalog_user' }, { relname: 'catalog_user_pkey' }]"
*/

SELECT table_schema, table_name FROM information_schema.tables WHERE table_name LIKE 'catalog_%' ORDER BY table_name;
SELECT column_name, ordinal_position, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'catalog_user' ORDER BY ordinal_position;
SELECT table_name, column_name FROM information_schema.columns WHERE table_name = 'catalog_note';
SELECT relname FROM pg_catalog.pg_class WHERE relname LIKE 'catalog_%' ORDER BY relname;
