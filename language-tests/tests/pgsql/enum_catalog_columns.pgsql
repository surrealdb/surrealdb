/**
[env]
namespace = true
database = true
imports = ["pgsql/_enum_schema.surql"]

[test]
reason = "A column of a named enum type is USER-DEFINED in information_schema.columns with the enum as its udt_name, and its atttypid in pg_attribute is the oid of the enum type"

[[test.results]]
value = "[{ column_name: 'm', data_type: 'USER-DEFINED', udt_name: 'mood' }]"

[[test.results]]
value = "[{ type: 'mood' }]"

[[test.results]]
value = "[{ same: true }]"
*/

SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_name = 'person' AND column_name = 'm';

SELECT format_type(a.atttypid, a.atttypmod) AS type FROM pg_attribute a WHERE a.attname = 'm';

SELECT a.atttypid = t.oid AS same FROM pg_attribute a, pg_type t WHERE a.attname = 'm' AND t.typname = 'mood';
