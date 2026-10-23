/**
[env]
namespace = true
database = true

[test]
reason = "The TypeORM schema introspection queries that build a regclass from quote_ident of information_schema names and read table and column comments evaluate over rows that exist and over an empty match"

[[test.results]]
value = "[{ table_comment: NULL, table_name: 'tables', table_schema: 'information_schema' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ column_comment: NULL, table_name: 'tables' }]"
*/

SELECT "table_schema", "table_name", obj_description((quote_ident("table_schema") || '.' || quote_ident("table_name"))::regclass, 'pg_class') AS table_comment FROM "information_schema"."tables" WHERE ("table_schema" = 'information_schema' AND "table_name" = 'tables') OR ("table_schema" = 'public' AND "table_name" = 'CompatPost');
SELECT "table_schema", "table_name", obj_description((quote_ident("table_schema") || '.' || quote_ident("table_name"))::regclass, 'pg_class') AS table_comment FROM "information_schema"."tables" WHERE ("table_schema" = 'public' AND "table_name" = 'CompatUser') OR ("table_schema" = 'public' AND "table_name" = 'CompatPost');
SELECT "table_name", col_description((quote_ident('postgres') || '.' || quote_ident("table_schema") || '.' || quote_ident("table_name"))::regclass::oid, 1) AS column_comment FROM "information_schema"."tables" WHERE "table_schema" = 'information_schema' AND "table_name" = 'tables';
