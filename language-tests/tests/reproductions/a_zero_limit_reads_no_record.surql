/**
[env]
namespace = true
database = true

[test]
reason = """
A zero `LIMIT` reads no record. A statement whose limit evaluates to zero
returns no row, and a read of storage beneath it -- a table scan, a record id
written down or resolved by the run, a record range -- checks that its table
exists and that the statement may select from it, and then reads nothing. So
no record is judged by the table's permissions, its `COMPUTED` fields, the
statement's predicates or its projection, and none of the errors they would
raise is raised. That holds whether the limit is pushed into the scan or kept
above it, as it is when the predicate reads a computed field.

A source that is not a read of storage yields its rows whatever the limit, and
they are judged: an array's predicate still fails. `START` alone reads as
usual, judging the rows it skips. A table that does not exist is refused.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "There was a problem running the concat() function. no such method found for the none type"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "The table 'missing' does not exist"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE FIELD bad ON t COMPUTED <int> 'x';
CREATE t:one SET a = 1 RETURN NONE;
DEFINE FIELD label ON u COMPUTED 1;
CREATE u:1 RETURN NONE;

-- A table scan, its limit pushed into it or kept above a predicate that reads
-- a computed field.
SELECT * FROM t LIMIT 0;
SELECT * FROM u WHERE $nope.concat([]) LIMIT 0;
SELECT * FROM u WHERE $nope.concat([]) LIMIT 1;

-- A record id written down, and the same record id resolved by the run.
SELECT * FROM t:one LIMIT 0;
SELECT * FROM array::at(array::shuffle([t:one]), 0) LIMIT 0;
SELECT * FROM array::at(array::shuffle([t:one]), 0) WHERE a = 1 LIMIT 0;

-- `START` alone judges the rows it skips.
SELECT * FROM array::at(array::shuffle([t:one]), 0) WHERE a = 1 START 5;

-- A source that is not a read of storage is judged whatever the limit.
SELECT * FROM array::shuffle([1]) WHERE <int> 'x' LIMIT 0;

-- A table that does not exist is refused.
SELECT * FROM missing LIMIT 0;
