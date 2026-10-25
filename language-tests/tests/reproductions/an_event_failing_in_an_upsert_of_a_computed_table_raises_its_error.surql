/**
[env]
namespace = true
database = true

[test]
reason = """
	An UPSERT whose creating attempt fails in one of its table's events raises
	the event's error, whether the statement names its table or computes it.
	A write whose table is computed runs each table's stages as a unit of
	their own, and the events unit runs inside it; the failure leaves the
	stages' unit, not the statement, so the statement's own failure handlers
	see it once.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event raises: Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Error while processing event raises: Could not cast into `int` using input `'x'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event throws: An error occurred: boom"

[[test.results]]
error = "Error while processing event throws: An error occurred: boom"

[[test.results]]
error = "Error while processing event throws: An error occurred: boom"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

DEFINE TABLE audit SCHEMALESS;
DEFINE TABLE q SCHEMALESS;
DEFINE EVENT raises ON q WHEN <int> "x" THEN (CREATE audit SET tag = 'q');
UPSERT q:1 SET a = 2;
UPSERT type::record('q' + '', 1) SET a = 2;
DEFINE TABLE r SCHEMALESS;
DEFINE EVENT throws ON r WHEN true THEN { THROW 'boom' };
UPSERT r:1 SET a = 2;
UPSERT type::record('r' + '', 1) SET a = 2;
CREATE type::record('r' + '', 2) SET a = 2;
UPDATE type::record('r' + '', 1) SET a = 3;
SELECT * FROM q, r, audit;
