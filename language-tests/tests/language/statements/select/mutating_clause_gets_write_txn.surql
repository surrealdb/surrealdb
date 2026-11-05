/**
[env]
namespace = true
database = true

[test]
reason = """
A clause the read-only predicate must inspect: the transaction type is chosen
from the statement's syntax before it is planned, so a mutation in LIMIT (or
START, VERSION, TIMEOUT, OMIT, FETCH) has to force a write transaction. When
the predicate missed these clauses the statement ran on a read transaction and
the write was refused, which also made the same statement behave differently
inside an explicit BEGIN block, since that opens a write transaction
unconditionally.
"""

[[test.results]]
value = "[{ id: person:1 }]"

[[test.results]]
value = "[{ id: person:2 }]"

[[test.results]]
value = "[{ id: person:1 }]"

[[test.results]]
value = "1"
*/

CREATE person:1;
CREATE person:2;
-- The LIMIT expression both mutates and yields the row count.
SELECT id FROM person LIMIT (CREATE ONLY log:l SET n = 1).n;
RETURN array::len((SELECT id FROM log));
