/**
[env]

[test]
reason = """
	A block, an `IF` branch and a nested statement over `$parent` evaluate
	against the row that holds them, so a computed field they read is the
	computed value.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, c: 2, id: t:1 }]"

[[test.results]]
value = "[{ x: 2 }]"

[[test.results]]
value = "[{ x: 2 }]"

[[test.results]]
value = "[{ x: 2 }]"

[[test.results]]
value = "[{ id: t:1 }]"

*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD a ON t TYPE int;
DEFINE FIELD c ON t TYPE int COMPUTED a + 1;
CREATE t:1 SET a = 1;

SELECT { RETURN c } AS x FROM t;
SELECT (IF a > 0 { c } ELSE { 0 }) AS x FROM t;
SELECT (SELECT VALUE c FROM ONLY $parent) AS x FROM t;
SELECT id FROM t WHERE { c } > 1;
