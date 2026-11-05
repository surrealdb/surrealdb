/**
[env]
namespace = true
database = true
imports = ["pgsql/_enum_schema.surql"]

[test]
reason = "A SQL read of an enum column returns its labels, and equality and IN compare them"

[[test.results]]
value = "[{ m: 'happy', name: 'a' }, { m: 'sad', name: 'b' }, { m: 'ok', name: 'c' }]"

[[test.results]]
value = "[{ name: 'c' }]"

[[test.results]]
value = "[{ name: 'a' }, { name: 'b' }]"

[[test.results]]
value = "[{ n: 2 }]"

[[test.results]]
value = "[{ enumlabel: 'sad' }, { enumlabel: 'ok' }, { enumlabel: 'happy' }]"

[[test.results]]
value = "[{ typname: 'mood', typtype: 'e' }]"

*/

SELECT name, m FROM person ORDER BY name;

SELECT name FROM person WHERE m = 'ok';

SELECT name FROM person WHERE m IN ('sad', 'happy') ORDER BY name;

SELECT count(*) AS n FROM person WHERE m <> 'ok';

SELECT enumlabel FROM pg_enum ORDER BY enumsortorder;

SELECT typname, typtype FROM pg_type WHERE typtype = 'e';
