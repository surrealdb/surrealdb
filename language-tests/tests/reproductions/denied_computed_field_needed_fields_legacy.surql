/**
[env]
planner-strategy = ["compute-only"]

[env.capabilities]
deny-functions = ["math::max"]

[test]
reason = """
Half of a pair. Same queries as
`denied_computed_field_needed_fields_streaming.surql`, pinned to the legacy
compute path; the two files' expected results are the divergence.

A table carries a COMPUTED field whose expression is denied, and no statement
here reads that field. Legacy materialises the whole document before
evaluating a WHERE, so any WHERE at all reaches the denied field and errors —
whether the predicate matches the row or not, and whether an index serves it
or not. The streaming scan pipeline computes only the fields the query needs,
so it never touches the field and answers normally.

This diverges on an ordinary matching query, not only on an empty one, which
is what separates it from the empty-scan boundary in the sibling pair.

Two controls place the boundary: a projection with no WHERE agrees, because
legacy computes a COMPUTED field only on a demand it sees and nothing here
demands it; and an empty index range agrees, because no row is materialised
on either path.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5 }]"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
value = "[]"
*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET a = 5;
DEFINE FIELD bad ON t COMPUTED math::max([1, 2]);

-- Control: a projection with no WHERE agrees across engines, because nothing
-- demands the denied field.
SELECT a FROM t;

-- A WHERE on an unrelated field. The first admits no row and the second
-- admits one, and legacy errors on both: the document is materialised before
-- the predicate is evaluated either way.
SELECT a FROM t WHERE a > 100;
SELECT a FROM t WHERE a > 0;

-- An index on the filtered field does not change which fields are
-- materialised once a row is a candidate.
DEFINE INDEX ta ON t FIELDS a;
SELECT a FROM t WHERE a = 5;

-- Control: an empty index range agrees, because no row is materialised.
SELECT a FROM t WHERE a > 1000;
