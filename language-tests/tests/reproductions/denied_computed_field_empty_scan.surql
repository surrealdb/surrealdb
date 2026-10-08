/**
[env]

[env.capabilities]
deny-functions = ["math::max"]

[test]
reason = """
A table carries a COMPUTED field whose expression is denied. An operator that
reads no row is selected for a statement that admits none, so `WHERE false`,
`LIMIT 0` and `WHERE a IN []` return `[]` without materialising a row or raising
the denial.

No denied code runs, so this is a matter of what the statement reports rather
than a bypass. Recorded so that a change to the boundary is visible, not as a
statement that this is the settled answer.

The baseline read still errors, which pins the deny list as genuinely in force.
The final case is the control that places the boundary: an empty *index* range
likewise materialises no row.
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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

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
