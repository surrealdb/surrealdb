/**
[env]
namespace = true
database = true
backend = ["rocksdb", "surrealkv"]
versioned = true

[test]
reason = """
	A KNN whose VERSION is a database parameter defined after the instant it
	holds. The enclosing version scope resolves the parameter once, outside the
	snapshot, and every read under it is at that stamp: the KNN returns both
	rows, as the table scan beside it does. Re-evaluating the parameter inside
	the snapshot would find no such parameter, since it did not exist yet.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ embedding: [0.1f, 0.1f, 0.1f], id: pt:1 }]"

[[test.results]]
value = "[{ embedding: [0.2f, 0.2f, 0.2f], id: pt:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[pt:1, pt:2]"

[[test.results]]
value = "[pt:1, pt:2]"
*/

DEFINE INDEX pt_emb ON pt FIELDS embedding HNSW DIMENSION 3 DIST EUCLIDEAN;
CREATE pt:1 SET embedding = [0.1f, 0.1f, 0.1f];
CREATE pt:2 SET embedding = [0.2f, 0.2f, 0.2f];
SLEEP 10ms;
DEFINE PARAM $hist VALUE time::now() PERMISSIONS FULL;
SELECT VALUE id FROM pt WHERE embedding <|2,40|> [0f, 0f, 0f] ORDER BY id VERSION $hist;
SELECT VALUE id FROM pt ORDER BY id VERSION $hist;
