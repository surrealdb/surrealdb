/**
[env]
namespace = true
database = true

[test]
reason = "Records that unbounded resident-memory growth is RocksDB-specific (memtables, blob files and the heap behind them). Vector rows must still index and be searchable on every engine, which the queries check."
issue = 7424

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 200 }]"

[[test.results]]
value = "['doc:10']"
*/

DEFINE TABLE doc SCHEMALESS;
DEFINE INDEX hx ON doc FIELDS v HNSW DIMENSION 3 DIST EUCLIDEAN TYPE F32;
FOR $i IN 0..200 { CREATE type::record('doc', $i) SET v = [$i, $i * 2, $i * 3] RETURN NONE; };
SELECT count() FROM doc GROUP ALL;
(SELECT id FROM doc WHERE v <|1,40|> [10, 20, 30]).map(|$r| <string>$r.id);
