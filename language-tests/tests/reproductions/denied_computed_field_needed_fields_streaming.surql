/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[env.capabilities]
deny-functions = ["math::max"]

[test]
reason = """
Half of a pair. Same queries as
`denied_computed_field_needed_fields_legacy.surql`, pinned to the streaming
planner; the two files' expected results are the divergence.

A table carries a COMPUTED field whose expression is denied, and no statement
here reads that field. The scan pipeline resolves only the fields the query
needs, so the field is never computed and every statement answers normally;
legacy materialises the whole document to evaluate a WHERE and errors on all
three.

This is the widest member of the family: it diverges on an ordinary matching
query with no constant, no fold and no elimination anywhere in the statement,
so neither the folder nor the empty-scan boundary explains it. Unfixed;
recorded so a change to which fields a scan resolves is visible on both sides.

Two controls place the boundary: a projection with no WHERE agrees, because
legacy computes a COMPUTED field only on a demand it sees; and an empty index
range agrees, because no row is materialised on either path. Neither control
proves the deny list is in force, so the sibling pair
`denied_computed_field_empty_scan_*` keeps the baseline read that does.
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
value = "[]"

[[test.results]]
value = "[{ a: 5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5 }]"

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
-- admits one, and the streaming pipeline answers both: the denied field is
-- not among the fields either statement needs.
SELECT a FROM t WHERE a > 100;
SELECT a FROM t WHERE a > 0;

-- An index on the filtered field does not change which fields are
-- materialised once a row is a candidate.
DEFINE INDEX ta ON t FIELDS a;
SELECT a FROM t WHERE a = 5;

-- Control: an empty index range agrees, because no row is materialised.
SELECT a FROM t WHERE a > 1000;
