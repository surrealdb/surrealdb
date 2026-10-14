/**
[env]
namespace = true
database = true
imports = ["pgsql/_join_lookup_errors.surql"]

[test]
reason = "A writing statement that reads a second table through its FROM or USING clause reads that table whole: the fallible computed field of the table raises on the row no key reaches although the statement holds only 50 rows, and a statement that raises writes nothing."

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[{ c: 50, s: 1225 }]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[{ c: 50 }]"
*/

UPDATE f SET n = 1 FROM e WHERE f.k = e.k;

SELECT count(*) AS c, sum(n) AS s FROM f;

DELETE FROM f USING e WHERE f.k = e.k;

SELECT count(*) AS c FROM f;
