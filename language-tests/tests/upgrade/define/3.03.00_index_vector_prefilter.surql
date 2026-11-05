/**
[env]
imports = ["upgrade/define/3.03.00_index_vector_prefilter_import.surql"]

[test]
requires = ["diskann"]
upgrade = true
introduced = ">=3.3.0"
reason = """
An HNSW or DiskANN index written before the doc-ID → elements mapping existed
has none of its keys, so the exact prefilter tier must score the allow-list
members from their records rather than from the index, and return the true
filtered neighbours.

The plan assertion and a post-upgrade write live in
3.03.00_index_vector_prefilter_current.surql."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ dist: 2f, id: pts_h:1 }, { dist: 12f, id: pts_h:3 }]"

[[test.results]]
value = "[{ dist: 2f, id: pts_d:1 }, { dist: 12f, id: pts_d:3 }]"

*/
LET $pt = [2, 3, 4, 5];
-- The two nearest category 'a' rows; pts_h:5 is nearer but filtered out.
SELECT id, vector::distance::knn() AS dist FROM pts_h
	WHERE category = 'a' AND point <|2,100|> $pt ORDER BY dist;
-- The same through the DiskANN index.
SELECT id, vector::distance::knn() AS dist FROM pts_d
	WHERE category = 'a' AND point <|2,100|> $pt ORDER BY dist;
