/**
[env]
namespace = true
database = true
backend = ["rocksdb", "surrealkv"]
versioned = true
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
	A divergence, pinned in a pair rather than fixed: a KNN whose VERSION is a
	database parameter defined after the instant it holds. The enclosing
	version scope resolves the parameter once, outside the snapshot, and a
	scan reads at that stamp; re-evaluating the parameter inside the snapshot
	finds no such parameter, since it did not exist yet. The streaming engine
	reads the KNN at the scope's stamp and returns both rows, as the table scan
	beside it does. The per-row engine returns no rows for the KNN. The
	streaming answer is correct; the per-row engine is being retired and is
	not fixed here.
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
