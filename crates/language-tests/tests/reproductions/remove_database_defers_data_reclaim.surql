/**
[env]
clean = true
namespace = "reclaim"
database = "data"
timeout = 5000

[test]
reason = """
REMOVE DATABASE deletes only the catalog definition and enqueues the orphaned
data prefix for the background reclaim task, so the statement's work is bounded
by the catalog rather than by how much data the database held. The database is
gone from the catalog the moment the statement commits, and its rows are out of
reach through its name from that moment too, because every path to a record
resolves the database through the catalog before it builds a key.
"""

[[test.results]]
match = "array::len($result) == 500"

[[test.results]]
value = "[{ count: 500 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, databases: {  }, users: {  } }"

[[test.results]]
error = "The database 'data' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

*/

-- A database with enough data that removing it row by row would be visible work
CREATE |person:500|;
SELECT count() FROM person GROUP ALL;

-- The removal itself, which must complete well inside the test timeout
REMOVE DATABASE data;

-- The database is gone from the catalog immediately
INFO FOR NS;
REMOVE DATABASE data;
REMOVE DATABASE IF EXISTS data;

-- The rows are still on disk until the reclaim task destroys them, and are
-- unreachable through the name the removal took away
SELECT * FROM person;
SELECT count() FROM person GROUP ALL;
