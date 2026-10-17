/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
`DEFINE TABLE OVERWRITE … AS SELECT` empties the view table before it fills it
again, and the views that read it stay registered on it: `INFO FOR TABLE` still
lists them, a write to the source still reaches them, and `REMOVE TABLE` of the
redefined view is still refused. Each row the view held is removed through the
table's order before the new rows arrive through it, so every view over it — a
plain one, a grouped one, one recalculating an extreme, one that also reads
another table, and one two levels down — holds what it would hold had the new
rows been written from the start. A table that held rows of its own before it
was redefined as a view retires them the same way, and a view nothing reads
whose table has a change feed records the removal of each of its rows there.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['g', 'mixed', 'top', 'v2']"

[[test.results]]
value = "[{ a: 20, id: v1:2 }]"

[[test.results]]
value = "[{ a: 20, id: v2:2 }]"

[[test.results]]
value = "[{ a: 20, id: deep:2 }]"

[[test.results]]
value = "[{ c: 1, s: 20f }]"

[[test.results]]
value = "[{ m: 20 }]"

[[test.results]]
value = "[{ c: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 20, id: v2:2 }, { a: 30, id: v2:3 }]"

[[test.results]]
value = "[{ a: 20, id: deep:2 }, { a: 30, id: deep:3 }]"

[[test.results]]
value = "[{ c: 2, s: 50f }]"

[[test.results]]
value = "[{ m: 30 }]"

[[test.results]]
value = "[{ c: 3 }]"

[[test.results]]
error = "Invalid query: Cannot delete table `v1` on which a view is defined, table(s) `g`, `mixed`, `top`, `v2` are defined as a view on this table."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 3, id: over_plain:3 }]"

[[test.results]]
value = "['over_plain']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = "array::any($result, |$entry| array::any($entry.changes, |$c| $c.delete.id == fed:2)) && array::any($result, |$entry| array::any($entry.changes, |$c| $c.update.a == 3))"
*/

DEFINE TABLE src SCHEMALESS;
DEFINE TABLE other SCHEMALESS;
CREATE src:1 SET a = 1 RETURN NONE;
CREATE src:2 SET a = 2 RETURN NONE;
CREATE other:1 SET a = 100 RETURN NONE;
DEFINE TABLE v1 AS SELECT a FROM src;
DEFINE TABLE v2 AS SELECT a FROM v1;
DEFINE TABLE deep AS SELECT a FROM v2;
DEFINE TABLE g AS SELECT count() AS c, math::sum(a) AS s FROM v1 GROUP ALL;
DEFINE TABLE top AS SELECT math::max(a) AS m FROM v1 GROUP ALL;
DEFINE TABLE mixed AS SELECT count() AS c FROM v1, other GROUP ALL;
-- The redefinition replaces every row v1 holds.
DEFINE TABLE OVERWRITE v1 AS SELECT a * 10 AS a FROM src WHERE a > 1;
object::keys((INFO FOR TABLE v1).tables);
SELECT * FROM v1 ORDER BY id;
SELECT * FROM v2 ORDER BY id;
SELECT * FROM deep ORDER BY id;
SELECT c, s FROM g;
SELECT m FROM top;
SELECT c FROM mixed;
-- A write to the source still reaches every view over the redefined one.
CREATE src:3 SET a = 3 RETURN NONE;
SELECT * FROM v2 ORDER BY id;
SELECT * FROM deep ORDER BY id;
SELECT c, s FROM g;
SELECT m FROM top;
SELECT c FROM mixed;
REMOVE TABLE v1;
-- A table holding rows of its own, redefined as a view, retires them through the views reading it.
DEFINE TABLE plain SCHEMAFULL;
DEFINE FIELD a ON plain TYPE int;
CREATE plain:1 SET a = 5 RETURN NONE;
DEFINE TABLE over_plain AS SELECT a FROM plain;
DEFINE TABLE OVERWRITE plain AS SELECT a FROM src WHERE a = 3;
SELECT * FROM over_plain ORDER BY id;
object::keys((INFO FOR TABLE plain).tables);
-- A view nothing reads still retires its rows through its order where its table has a change feed.
DEFINE TABLE fed CHANGEFEED 1h AS SELECT a FROM src WHERE a = 2;
DEFINE TABLE OVERWRITE fed CHANGEFEED 1h AS SELECT a FROM src WHERE a = 3;
SHOW CHANGES FOR TABLE fed SINCE 1;
