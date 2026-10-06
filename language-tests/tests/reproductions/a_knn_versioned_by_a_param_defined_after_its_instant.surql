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
	snapshot, and a scan reads at that stamp; re-evaluating the parameter inside
	the snapshot finds no such parameter, since it did not exist yet. The KNN
	reads at the scope's stamp and returns both rows, as the table scan beside
	it does, on every planner strategy.
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
