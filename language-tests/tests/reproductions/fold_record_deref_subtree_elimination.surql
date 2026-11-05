/**
[env.capabilities]
deny-functions = ["math::max"]

[test]
reason = "Reading a field of a record id fetches that record and evaluates its computed fields, so a field read may only be discarded by a fold when the row it reads from is itself a record"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: target:1, x: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, id: t:1, ref: target:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
value = "[]"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
value = "[]"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

*/

DEFINE TABLE target SCHEMALESS;
CREATE target:1 SET x = 1;
DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET ref = target:1, a = 5;

-- Defined after the row exists: a COMPUTED field is evaluated on read, so
-- defining it earlier would fail the CREATE above rather than the reads below.
DEFINE FIELD bad ON target COMPUTED math::max([1, 2]);

-- `ref` holds a record id, so the second part fetches target:1 and evaluates
-- its computed field, which calls the denied function. Nothing in the idiom
-- AST shows that, so the operand cannot be treated as inert.
SELECT * FROM t WHERE ref.bad > 0;
SELECT * FROM t WHERE ref.bad IN [];
SELECT * FROM t WHERE ref.bad > 0 AND false;

-- A single field read comes off the row itself, which is an object, so it
-- dereferences nothing and still collapses.
SELECT * FROM t WHERE a IN [];

-- A single field read is only safe to discard when the row is a record. Over
-- a source that yields an array, field access recurses into the elements and
-- fetches any record id it finds, so `bad` dereferences target:1 here despite
-- being one part long.
SELECT * FROM [[target:1]] WHERE bad > 0;
SELECT * FROM [[target:1]] WHERE bad IN [];

-- Every source is held to the same standard: a scalar alongside the table
-- cannot expose a record, but an array can, and one dereferencing source is
-- enough to keep the condition.
SELECT * FROM t, 1 WHERE a IN [];
SELECT * FROM t, [target:1] WHERE bad IN [];
