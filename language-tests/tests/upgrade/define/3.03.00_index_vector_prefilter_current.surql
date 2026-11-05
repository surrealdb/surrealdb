/**
[env]
imports = ["upgrade/define/3.03.00_index_vector_prefilter_import.surql"]

[test]
upgrade = true
introduced = ">=3.3.0"
reason = """
Pins that the filtered KNN over an index written before the doc-ID → elements
mapping still takes the exact prefilter tier — an unusable allow-list would fall
back to a graph walk and could return the same rows — and that it stays correct
while the new binary maintains the old index: a record written after the upgrade
must be found, although the old index keeps no mapping for it either."""
version = ">=3.4.0-a"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''string::contains($result, "KnnScan [ctx: Db] [index: hnsw_h") AND string::contains($result, "prefilter_tier: exact") AND string::contains($result, "BitmapIndexScan [ctx: Db] [index: cat_h")'''

[[test.results]]
match = '''string::contains($result, "KnnScan [ctx: Db] [index: dann_d") AND string::contains($result, "prefilter_tier: exact") AND string::contains($result, "BitmapIndexScan [ctx: Db] [index: cat_d")'''

[[test.results]]
value = "[{ category: 'a', id: pts_h:6, point: [2f, 3f, 4f, 6f] }]"

[[test.results]]
value = "[{ category: 'a', id: pts_d:6, point: [2f, 3f, 4f, 6f] }]"

[[test.results]]
value = "[{ dist: 1f, id: pts_h:6 }, { dist: 2f, id: pts_h:1 }]"

[[test.results]]
value = "[{ dist: 1f, id: pts_d:6 }, { dist: 2f, id: pts_d:1 }]"

*/
LET $pt = [2, 3, 4, 5];
EXPLAIN ANALYZE SELECT id FROM pts_h WHERE category = 'a' AND point <|2,100|> $pt;
EXPLAIN ANALYZE SELECT id FROM pts_d WHERE category = 'a' AND point <|2,100|> $pt;
-- pts_x:6 sits one unit from the probe, so it must now lead.
CREATE pts_h:6 SET point = [2f, 3f, 4f, 6f], category = 'a';
CREATE pts_d:6 SET point = [2f, 3f, 4f, 6f], category = 'a';
SELECT id, vector::distance::knn() AS dist FROM pts_h
	WHERE category = 'a' AND point <|2,100|> $pt ORDER BY dist;
SELECT id, vector::distance::knn() AS dist FROM pts_d
	WHERE category = 'a' AND point <|2,100|> $pt ORDER BY dist;
