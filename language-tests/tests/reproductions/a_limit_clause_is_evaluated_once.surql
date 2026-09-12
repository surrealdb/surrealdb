/**
[env]
namespace = true
database = true

[test]
reason = """
A scan whose residual filter keeps the `Limit` operator above it took the
statement's compiled `LIMIT` as a per-batch read-size hint, and evaluated it
again when its stream opened. The clause the statement wrote once then ran
twice — once per scan beneath the operator, so once more again for every branch
of a merged union.

The hint sizes a read and never changes a row, so it is a plan-time number
taken from a literal clause; a clause that is not a literal yields no hint
rather than a second evaluation.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: counter:c, n: 0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, p: 'a' }]"

[[test.results]]
value = "[{ id: person:2, p: 'b' }]"

[[test.results]]
value = "[{ id: person:3, p: 'c' }]"

[[test.results]]
value = "[{ id: person:4, p: 'd' }]"

[[test.results]]
value = "[person:1, person:2]"

[[test.results]]
value = "[1]"
*/

DEFINE TABLE counter SCHEMALESS;
CREATE counter:c SET n = 0;

-- Counts its own invocations, so the statement below reports how many times
-- its `LIMIT` was evaluated.
DEFINE FUNCTION fn::bump() { UPDATE counter:c SET n += 1; RETURN 2; };

DEFINE TABLE person SCHEMALESS;
DEFINE INDEX px ON person FIELDS p;
CREATE person:1 SET p = 'a';
CREATE person:2 SET p = 'b';
CREATE person:3 SET p = 'c';
CREATE person:4 SET p = 'd';

-- The index covers the ordering and the `IN` leaves a residual filter, so the
-- scan keeps its `Limit` operator and takes a batch hint.
SELECT VALUE id FROM person WHERE p IN ['a', 'b', 'c', 'd'] ORDER BY p LIMIT fn::bump();
SELECT VALUE n FROM counter:c;
