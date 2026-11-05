/**
[env.capabilities]
deny-functions = ["math::max"]

[test]
reason = "Materialising a row evaluates the table's COMPUTED fields before a condition observes it, so a fold may only discard a field read when reading a row of that table evaluates nothing"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, id: plain:1 }]"

[[test.results]]
value = "[]"

*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET a = 5;

-- Defined after the row exists: a COMPUTED field is evaluated on read, so
-- defining it earlier would fail the CREATE above.
DEFINE FIELD bad ON t COMPUTED math::max([1, 2]);

-- Reading `bad` computes it, and the denied call it holds is not visible in
-- the idiom AST, so an eliminating fold must not discard the read.
SELECT * FROM t WHERE bad IN [];
SELECT * FROM t WHERE bad > 0 AND false;
SELECT * FROM t WHERE false OR bad > 0;

-- A table that computes nothing on read keeps the elimination.
DEFINE TABLE plain SCHEMALESS;
CREATE plain:1 SET a = 5;
SELECT * FROM plain WHERE a IN [];
