/**
[env]
namespace = true
database = true

[test]
reason = """
A record-id source that only the run resolves is read by the same lookup as a
record id written down, and the lookup is what judges the row: the table
`PERMISSIONS` clause, the `COMPUTED` fields and the pushed predicate. A zero
limit emits nothing, but those are evaluations the statement owes whether or
not the row is emitted, so the lookup reads and judges first and discards
afterwards. Returning before it left the two plans for the same query
answering differently.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE FIELD bad ON t COMPUTED <int> 'x';
CREATE t:one SET a = 1 RETURN NONE;

-- The record id written down: the lookup reads and judges, then discards.
SELECT * FROM t:one LIMIT 0;

-- The same record id, resolved by the run. The two plans answer alike.
SELECT * FROM array::at(array::shuffle([t:one]), 0) LIMIT 0;

-- A pushed predicate is owed the same way.
SELECT * FROM array::at(array::shuffle([t:one]), 0) WHERE a = 1 LIMIT 0;
SELECT * FROM array::at(array::shuffle([t:one]), 0) WHERE a = 1 START 5;
