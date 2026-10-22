/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
The searches that only an index can answer, a full-text match and a
nearest-neighbour search, have no index to run on in the transaction that
defined one over rows committed before it, as they have none over an index
still building: the match is refused, and the nearest neighbours are ranked
over the rows of the table, as they are while a vector index builds. The
index answers both once the transaction has committed.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "There was no suitable index supporting the expression: t @@ 'alpha'"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[doc:1, doc:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[doc:1, doc:2]"
*/

DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
DEFINE TABLE doc SCHEMALESS;
CREATE doc:1 SET t = 'alpha one', e = [1.0, 0.0] RETURN NONE;
CREATE doc:2 SET t = 'beta two', e = [0.0, 1.0] RETURN NONE;

-- A full-text match runs on its index and on nothing else. The transaction
-- that defined the index has none to run it on until it commits, as a match
-- over an index still building has none, and the match is refused.
BEGIN;
DEFINE INDEX ft ON doc FIELDS t FULLTEXT ANALYZER simple BM25;
SELECT VALUE id FROM doc WHERE t @@ 'alpha';
COMMIT;
BEGIN;
DEFINE INDEX ft ON doc FIELDS t FULLTEXT ANALYZER simple BM25;
COMMIT;
SELECT VALUE id FROM doc WHERE t @@ 'alpha' ORDER BY id;

-- A nearest-neighbour search is not run on the index there either, the graph
-- the build wrote being past the transaction's snapshot. It ranks the rows of
-- the table, which are every row the index will find once the transaction has
-- committed.
BEGIN;
DEFINE INDEX h ON doc FIELDS e HNSW DIMENSION 2 DIST EUCLIDEAN;
SELECT VALUE id FROM doc WHERE e <|2,40|> [1.0, 1.0] ORDER BY id;
COMMIT;
SELECT VALUE id FROM doc WHERE e <|2,40|> [1.0, 1.0] ORDER BY id;
