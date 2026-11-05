/**
[env]
namespace = true
database = true
imports = ["pgsql/_composite_domain.surql"]

[test]
reason = "A domain over a composite type has that type as its base in pg_type.typbasetype and information_schema.domains, and a composite attribute of another composite type has that type's OID in pg_attribute"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ over_point: true, typtype: 'd' }]"

[[test.results]]
value = "[{ data_type: 'USER-DEFINED', domain_name: 'd_point', udt_name: 'point_t' }]"

[[test.results]]
value = "[{ attname: 'label', is_point: false }, { attname: 'at', is_point: true }]"

*/

CREATE TYPE labelled_t AS (label text, at point_t);
SELECT t.typtype, t.typbasetype = (SELECT oid FROM pg_type WHERE typname = 'point_t') AS over_point FROM pg_type t WHERE t.typname = 'd_point';
SELECT domain_name, data_type, udt_name FROM information_schema.domains WHERE domain_name = 'd_point';
SELECT a.attname, a.atttypid = (SELECT oid FROM pg_type WHERE typname = 'point_t') AS is_point FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'labelled_t' ORDER BY a.attnum;
