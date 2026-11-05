/**
[env]
namespace = true
database = true

[test]
reason = """
A view whose table name only the run knows — a block's variable, a loop
variable — is defined and filled from its sources as a view with a written
name is, materialized or aggregated, and kept up to date by later writes.
"""

[[test.results]]
value = "[{ a: 1, g: 'x', id: t:1 }]"

[[test.results]]
value = "[{ a: 2, g: 'x', id: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, g: 'x', id: v1:1 }, { a: 2, g: 'x', id: v1:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: v3:1 }, { a: 2, id: v3:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 2, g: 'x', id: v5:['x'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'v6' already exists"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 3, g: 'y', id: t:3 }]"

[[test.results]]
value = "[{ a: 1, id: v3:1 }, { a: 2, id: v3:2 }, { a: 3, id: v3:3 }]"

[[test.results]]
value = "[{ c: 2, g: 'x', id: v5:['x'] }, { c: 1, g: 'y', id: v5:['y'] }]"
*/
CREATE t:1 SET a = 1, g = 'x'; CREATE t:2 SET a = 2, g = 'x';
{ LET $n = 'v1'; DEFINE TABLE $n AS SELECT * FROM t; };
SELECT * FROM v1;
FOR $n IN ['v2', 'v3'] { DEFINE TABLE $n AS SELECT a FROM t; };
SELECT * FROM v3;
FOR $n IN ['v5'] { DEFINE TABLE $n AS SELECT count() AS c, g FROM t GROUP BY g; };
SELECT * FROM v5;
DEFINE INDEX ia ON v6 FIELDS a UNIQUE;
FOR $n IN ['v6'] { DEFINE TABLE $n AS SELECT a FROM t; };
SELECT * FROM v6 WHERE a = 2;
CREATE t:3 SET a = 3, g = 'y';
SELECT * FROM v3 ORDER BY id;
SELECT * FROM v5 ORDER BY g;
