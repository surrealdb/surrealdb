/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL pg_typeof names the type of a value"

[[test.results]]
value = "[{ a: 'boolean', b: 'text', c: 'unknown' }]"

*/

SELECT pg_typeof(true) AS a, pg_typeof(CAST('a' AS text)) AS b, pg_typeof(NULL) AS c;
