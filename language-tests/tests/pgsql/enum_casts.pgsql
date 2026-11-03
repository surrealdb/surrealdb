/**
[env]
namespace = true
database = true
imports = ["pgsql/_enum_schema.surql"]

[test]
reason = "PostgreSQL casts to an enum type judge membership, including inside arrays, and the enum type is named by format_type, to_regtype and regtype from its oid"

[[test.results]]
value = "[{ m: 'ok' }]"

[[test.results]]
error = "Could not cast into `mood`: Expected `mood ('sad' | 'ok' | 'happy')` but found `'nope'`"

[[test.results]]
value = "[{ m: 'happy' }]"

[[test.results]]
error = "Could not cast into `mood`: Expected `mood ('sad' | 'ok' | 'happy')` but found `'nope'`"

[[test.results]]
value = "[{ a: ['sad', 'ok'] }]"

[[test.results]]
error = "Could not cast into `array<mood>`: Expected `mood ('sad' | 'ok' | 'happy')` but found `'nope'`"

[[test.results]]
value = "[{ m: NULL }]"

[[test.results]]
value = "[{ name: 'c' }]"

[[test.results]]
value = "[{ name: 'mood' }]"

[[test.results]]
value = "[{ name: 'mood[]' }]"

[[test.results]]
value = "[{ same: true }]"

[[test.results]]
value = "[{ name: 'mood' }]"

[[test.results]]
value = "[{ name: 'integer' }]"
*/

SELECT 'ok'::mood AS m;

SELECT 'nope'::mood AS m;

SELECT CAST('happy' AS mood) AS m;

SELECT CAST('nope' AS mood) AS m;

SELECT '{sad,ok}'::mood[] AS a;

SELECT '{sad,nope}'::mood[] AS a;

SELECT NULL::mood AS m;

SELECT name FROM person WHERE m = 'ok'::mood;

SELECT format_type(oid, NULL) AS name FROM pg_type WHERE typname = 'mood';

SELECT format_type(typarray, NULL) AS name FROM pg_type WHERE typname = 'mood';

SELECT to_regtype('mood') = oid AS same FROM pg_type WHERE typname = 'mood';

SELECT oid::regtype AS name FROM pg_type WHERE typname = 'mood';

SELECT format_type(23, NULL) AS name;
