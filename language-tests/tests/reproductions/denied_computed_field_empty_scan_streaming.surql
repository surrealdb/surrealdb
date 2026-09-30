/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[env.capabilities]
deny-functions = ["math::max"]

[test]
reason = """
Half of a pair. Same queries as
`denied_computed_field_empty_scan_legacy.surql`, pinned to the streaming
planner; the two files' expected results are the divergence.

A table carries a COMPUTED field whose expression is denied. The streaming
planner selects an operator that reads no row for a statement that admits
none, so `WHERE false`, `LIMIT 0` and `WHERE a IN []` return `[]` where legacy
materialises a row and raises the denial.

No denied code runs on either side, so this is a divergence in what the
statement reports rather than a bypass; closing it means scanning
provably-empty results. Recorded so that a change to the boundary is visible,
not as a statement that either answer is the settled one.

The baseline read still errors, which pins the deny list as genuinely in
force. The final case is the control that places the boundary: an empty
*index* range agrees, because legacy never materialises a row there either.
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
