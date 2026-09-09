/**
[env]
planner-strategy = ["compute-only"]

[env.capabilities]
deny-functions = ["math::max"]

[test]
reason = """
Half of a pair. Same queries as
`denied_computed_field_empty_scan_streaming.surql`, pinned to the legacy
compute path; the two files' expected results are the divergence.

A table carries a COMPUTED field whose expression is denied, so any read that
materialises a row raises the denial. Legacy materialises a row before it can
decide the statement admits none, so a provably-empty statement errors here:
`WHERE false`, `LIMIT 0` and `WHERE a IN []` all reach the field. The
streaming planner selects an operator that reads nothing and returns `[]`.

No denied code runs on either side, so this is a divergence in what the
statement reports rather than a bypass. It is the shape with the widest
surface: the boundary is which operator the plan selects, with no constant
folding or subtree elimination anywhere in the statement.

The final case is the control that places the boundary: an empty *index*
range does not diverge, because legacy never materialises a row there either.
"""

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
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"
*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET a = 5;
DEFINE FIELD bad ON t COMPUTED math::max([1, 2]);

-- Baseline: a read that materialises the row reaches the denied field, so the
-- deny list is in force for everything below.
SELECT * FROM t;

-- Statements that admit no row. Nothing here folds a denied call or discards
-- a subtree holding one; the whole difference is whether a row is read before
-- the emptiness is known.
SELECT * FROM t WHERE false;
SELECT * FROM t LIMIT 0;
SELECT * FROM t WHERE a IN [];

-- Control: an empty index range agrees across engines.
DEFINE INDEX ta ON t FIELDS a;
SELECT * FROM t WHERE a > 1000;
