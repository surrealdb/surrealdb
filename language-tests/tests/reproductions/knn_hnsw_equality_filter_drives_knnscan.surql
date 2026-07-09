/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Plan-shape guard for `field <|k,ef|> $q AND <equality>` (HNSW). A common support misdiagnosis is that adding an equality filter makes the planner drive from the equality index and stop using the HNSW index. It does not: a KNN operator scores 800 and, more importantly, is stripped from the WHERE and MUST be consumed by a `KnnScan` (it can never be left as a residual). So even when an equality index exists on the filtered field (here `idx_category`, and the equality alone would only score 500), the plan is a single `KnnScan` on the HNSW index with the equality pushed down as a `predicate` (filtered-HNSW), NOT a `BTreeScan` on `idx_category`. See exec/planner/select/mod.rs::plan_knn_search_source and exec/index/analysis.rs::score. NOTE (performance, not asserted here): filtered-HNSW evaluates the predicate per visited candidate during graph traversal (idx/trees/hnsw/layer.rs::search_with_filter); a *selective* filter keeps the ef-sized result set from filling, so the search walks a large fraction of the graph, fetching a vector + full record per node. At scale this is the dominant cost, and pure-KNN latency is separately gated by the shared HNSW vector cache (SURREAL_HNSW_CACHE_SIZE, default 256 MiB)."

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ distance: 10f, id: pts:1 }, { distance: 30f, id: pts:3 }, { distance: 50f, id: pts:5 }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id] {rows: 3}\n    Filter [ctx: Db] [predicate: category = 'a'] {rows: 3}\n        KnnScan [ctx: Db] [index: pt_hnsw, k: 3, ef: 40, dimension: 1, predicate: category = 'a'] {rows: 3}\n\nTotal rows: 3"'''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id] {rows: 3}\n    KnnScan [ctx: Db] [index: pt_hnsw, k: 3, ef: 40, dimension: 1] {rows: 3}\n\nTotal rows: 3''''
*/

{
	-- The HNSW index for the vector field...
	DEFINE INDEX pt_hnsw ON pts FIELDS point HNSW DIMENSION 1 DIST EUCLIDEAN TYPE F32;
	-- ...AND a regular index on the categorical filter field. Its presence is
	-- the point: the KNN must still drive the scan despite this index existing.
	DEFINE INDEX idx_category ON pts FIELDS category;
	INSERT INTO pts [
		{ id: pts:1,  point: [ 10f ],  category: "a" },
		{ id: pts:2,  point: [ 20f ],  category: "b" },
		{ id: pts:3,  point: [ 30f ],  category: "a" },
		{ id: pts:4,  point: [ 40f ],  category: "b" },
		{ id: pts:5,  point: [ 50f ],  category: "a" },
		{ id: pts:6,  point: [ 60f ],  category: "b" },
		{ id: pts:7,  point: [ 70f ],  category: "a" },
		{ id: pts:8,  point: [ 80f ],  category: "b" },
		{ id: pts:9,  point: [ 90f ],  category: "a" },
		{ id: pts:10, point: [ 100f ], category: "b" }
	];
	RETURN 'OK';
};

LET $q = [0f];

-- Filtered KNN: the 3 nearest rows with category = 'a' are pts:1, pts:3, pts:5.
-- pts:2 (distance 20) is nearer than pts:3 (30) but is correctly excluded.
SELECT id, vector::distance::knn() AS distance FROM pts
	WHERE point <|3,40|> $q AND category = "a"
	ORDER BY distance;

-- The plan MUST be a single KnnScan on pt_hnsw with `predicate: category = 'a'`
-- pushed into the HNSW search. It must NOT be a BTreeScan on idx_category.
EXPLAIN ANALYZE SELECT id FROM pts WHERE point <|3,40|> $q AND category = "a";

-- Contrast: pure KNN (no filter) is the same KnnScan without a predicate.
EXPLAIN ANALYZE SELECT id FROM pts WHERE point <|3,40|> $q;
