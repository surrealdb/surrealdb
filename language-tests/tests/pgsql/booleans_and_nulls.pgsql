/**
[env]
namespace = true
database = true
imports = ["pgsql/_schema.surql"]

[test]
reason = "PostgreSQL 3VL boolean logic and IS NULL / IS NOT NULL evaluation"

[[test.results]]
value = "[{ a: true, b: false, c: true }]"
*/

SELECT (TRUE AND NULL) IS NULL AS a, (FALSE AND NULL) AS b, (NULL OR TRUE) AS c;
