/**
[env]
namespace = true
database = true
imports = ["pgsql/_json_array_columns.surql"]

[test]
reason = "A json[] or jsonb[] column reports its declared array type: information_schema.columns reports it as an ARRAY of _json or _jsonb, and pg_attribute gives it the OID of that array type and its dimensions"

[[test.results]]
value = "[{ data_type: 'ARRAY', is_nullable: 'YES', table_name: 'ja_grid', udt_name: '_jsonb' }, { data_type: 'ARRAY', is_nullable: 'YES', table_name: 'ja_nullable', udt_name: '_jsonb' }, { data_type: 'ARRAY', is_nullable: 'NO', table_name: 'ja_required', udt_name: '_json' }]"

[[test.results]]
value = "[{ attndims: 2, attnotnull: false, atttypid: 3807, relname: 'ja_grid' }, { attndims: 1, attnotnull: false, atttypid: 3807, relname: 'ja_nullable' }, { attndims: 1, attnotnull: true, atttypid: 199, relname: 'ja_required' }]"

*/

SELECT table_name, data_type, udt_name, is_nullable FROM information_schema.columns WHERE column_name = 'docs' ORDER BY table_name;
SELECT c.relname, a.atttypid, a.attndims, a.attnotnull FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE a.attname = 'docs' ORDER BY c.relname;
