/**
[env]

[env.capabilities]
deny-functions = ["math::max"]

[test]
reason = """
A table carries a COMPUTED field whose expression is denied, and no statement
here reads that field. The scan resolves only the fields the query needs, so
the field is never computed and every statement answers normally.

This is the widest member of the family: it holds for an ordinary matching
query with no constant, no fold and no elimination anywhere in the statement.

Two controls place the boundary: a projection with no WHERE never computes the
field, and an empty index range materialises no row. Neither control proves the
deny list is in force, so `denied_computed_field_empty_scan.surql` keeps the
baseline read that does.
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
