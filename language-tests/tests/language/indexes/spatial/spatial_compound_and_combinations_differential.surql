/**
[env]
timeout = 30000

[test]
reason = "Differential compound and combined spatial query battery: compound SPATIAL indexes (multi-column equality prefix and array containment prefix), multi-predicate spatial AND conjunctions, BitmapFusion (B-Tree + SPATIAL), OR unions of spatial branches, ORDER BY id ASC/DESC with LIMIT/START, and hybrid HNSW vector KNN + SPATIAL prefilter — each paired with its unindexed / brute-force ground-truth twin."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[fleet:uk1]"

[[test.results]]
value = "[fleet:uk1]"

[[test.results]]
value = "[fleet:fr1, fleet:uk1]"

[[test.results]]
value = "[fleet:fr1, fleet:uk1]"

[[test.results]]
value = "[fleet:uk1, fleet:uk3]"

[[test.results]]
value = "[fleet:uk1, fleet:uk3]"

[[test.results]]
value = "[fleet:uk1, fleet:uk2, fleet:uk3]"

[[test.results]]
value = "[fleet:uk1, fleet:uk2, fleet:uk3]"

[[test.results]]
value = "[fleet:uk1, fleet:uk2, fleet:uk3]"

[[test.results]]
value = "[fleet:uk1, fleet:uk2, fleet:uk3]"

[[test.results]]
value = "[fleet:uk1, fleet:uk3]"

[[test.results]]
value = "[fleet:uk1, fleet:uk3]"

[[test.results]]
value = "[fleet:fr1, fleet:uk1, fleet:uk3]"

[[test.results]]
value = "[fleet:fr1, fleet:uk1, fleet:uk3]"

[[test.results]]
value = "[fleet:jp1, fleet:uk1, fleet:uk2, fleet:uk3]"

[[test.results]]
value = "[fleet:jp1, fleet:uk1, fleet:uk2, fleet:uk3]"

[[test.results]]
value = "[fleet:fr2, fleet:uk1]"

[[test.results]]
value = "[fleet:fr2, fleet:uk1]"

[[test.results]]
value = "[fleet:uk3, fleet:uk2, fleet:uk1, fleet:fr2, fleet:fr1]"

[[test.results]]
value = "[fleet:uk3, fleet:uk2, fleet:uk1, fleet:fr2, fleet:fr1]"

[[test.results]]
value = "[fleet:fr2, fleet:uk1]"

[[test.results]]
value = "[fleet:fr2, fleet:uk1]"

[[test.results]]
value = "[{ d: 0f, id: fleet:uk1 }, { d: 0.1414213562373095f, id: fleet:uk3 }]"

[[test.results]]
value = "[{ d: 0f, id: fleet:uk1 }, { d: 0.1414213562373095f, id: fleet:uk3 }]"

[[test.results]]
value = "[{ d: 0f, id: fleet:uk1 }, { d: 0.42426406871192857f, id: fleet:fr1 }]"

[[test.results]]
value = "[{ d: 0f, id: fleet:uk1 }, { d: 0.42426406871192857f, id: fleet:fr1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ d: 0f, id: fleet:uk1 }, { d: 0.1414213562373095f, id: fleet:uk3 }]"

[[test.results]]
value = "[{ d: 0f, id: fleet:uk1 }, { d: 0.1414213562373095f, id: fleet:uk3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[deliveries:d1]"

[[test.results]]
value = "[deliveries:d1]"

[[test.results]]
value = "3"

[[test.results]]
value = "2"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['UnionIndexScan', '1', 'IndexScan', 'idx_fleet_loc', 'loc INSIDE (-0.12, 51.51)']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Filter', 'UnionIndexScan', '1', 'DistinctRecords', 'idx_fleet_tenant_loc']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Filter', 'UnionIndexScan', '1', 'DistinctRecords', 'idx_fleet_tags_loc']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Filter', 'BitmapScan', '[loc, status]', 'BitmapAnd', 'idx_fleet_status', 'idx_fleet_loc', 'loc INSIDE (-0.12, 51.51)']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Filter', 'KnnScan', 'idx_fleet_vec', 'BitmapOr', 'idx_fleet_loc', 'loc INSIDE (-0.12, 51.51)']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Filter', 'BitmapScan', '[loc]', 'BitmapOr', 'loc INSIDE (-0.12, 51.51)', 'loc INSIDE (139.69, 35.68)']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[fleet:fr1, fleet:jp1, fleet:uk1, fleet:uk3]"

[[test.results]]
value = "[fleet:fr1, fleet:jp1, fleet:uk1, fleet:uk3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Filter', 'BitmapScan', NONE]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Filter', 'BitmapScan', NONE]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[fleet:fr1, fleet:uk1, fleet:uk3]"

[[test.results]]
value = "[fleet:fr1, fleet:uk1, fleet:uk3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Filter', 'BitmapScan', '[loc, status]', 'BitmapAnd']"

[[test.results]]
value = "[fleet:fr1, fleet:fr2, fleet:jp1, fleet:uk1, fleet:uk2, fleet:uk3]"

[[test.results]]
value = "[fleet:fr1, fleet:fr2, fleet:jp1, fleet:uk1, fleet:uk2, fleet:uk3]"
*/

DEFINE TABLE fleet SCHEMAFULL;
DEFINE FIELD tenant   ON fleet TYPE string;
DEFINE FIELD category ON fleet TYPE string;
DEFINE FIELD status   ON fleet TYPE string;
DEFINE FIELD tags     ON fleet TYPE array<string>;
DEFINE FIELD loc      ON fleet TYPE geometry<point>;
DEFINE FIELD vec      ON fleet TYPE array<float>;

DEFINE INDEX idx_fleet_status     ON fleet FIELDS status;
DEFINE INDEX idx_fleet_loc        ON fleet FIELDS loc SPATIAL;
DEFINE INDEX idx_fleet_tenant_loc ON fleet FIELDS tenant, category, loc SPATIAL;
DEFINE INDEX idx_fleet_tags_loc   ON fleet FIELDS tags[*], loc SPATIAL;
DEFINE INDEX idx_fleet_vec        ON fleet FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F64;

CREATE fleet:uk1 SET tenant = 'acme',   category = 'van',   status = 'open',   tags = ['express', 'cold'], loc = (-0.12, 51.51), vec = [1.0, 0.0] RETURN NONE;
CREATE fleet:uk2 SET tenant = 'acme',   category = 'truck', status = 'closed', tags = ['heavy'],           loc = (-0.15, 51.52), vec = [0.8, 0.2] RETURN NONE;
CREATE fleet:uk3 SET tenant = 'globex', category = 'van',   status = 'open',   tags = ['express'],         loc = (-0.10, 51.50), vec = [0.9, 0.1] RETURN NONE;
CREATE fleet:fr1 SET tenant = 'acme',   category = 'van',   status = 'open',   tags = ['express'],         loc = (2.35, 48.85),  vec = [0.7, 0.3] RETURN NONE;
CREATE fleet:fr2 SET tenant = 'globex', category = 'van',   status = 'closed', tags = ['standard'],        loc = (2.36, 48.86),  vec = [0.6, 0.4] RETURN NONE;
CREATE fleet:jp1 SET tenant = 'acme',   category = 'van',   status = 'open',   tags = ['express'],         loc = (139.69, 35.68), vec = [0.99, 0.01] RETURN NONE;

LET $uk_box = {
	type: "Polygon",
	coordinates: [[[-1.0, 51.0], [0.5, 51.0], [0.5, 52.0], [-1.0, 52.0], [-1.0, 51.0]]]
};
LET $fr_box = {
	type: "Polygon",
	coordinates: [[[1.5, 48.0], [3.5, 48.0], [3.5, 49.5], [1.5, 49.5], [1.5, 48.0]]]
};
LET $jp_box = {
	type: "Polygon",
	coordinates: [[[139.0, 35.0], [140.5, 35.0], [140.5, 36.5], [139.0, 36.5], [139.0, 35.0]]]
};
LET $eur_box = {
	type: "Polygon",
	coordinates: [[[-5.0, 45.0], [10.0, 45.0], [10.0, 55.0], [-5.0, 55.0], [-5.0, 45.0]]]
};
LET $london = (-0.12, 51.51);

-- 1. Compound SPATIAL index (`tenant = 'acme' AND category = 'van' AND loc INSIDE $uk_box`)
SELECT VALUE id FROM fleet
	WHERE tenant = 'acme' AND category = 'van' AND loc INSIDE $uk_box
	ORDER BY id;
SELECT VALUE id FROM fleet WITH NOINDEX
	WHERE tenant = 'acme' AND category = 'van' AND loc INSIDE $uk_box
	ORDER BY id;

-- 2. Compound SPATIAL index with `geo::distance`
SELECT VALUE id FROM fleet
	WHERE tenant = 'acme' AND category = 'van' AND geo::distance(loc, $london) < 500000.0
	ORDER BY id;
SELECT VALUE id FROM fleet WITH NOINDEX
	WHERE tenant = 'acme' AND category = 'van' AND geo::distance(loc, $london) < 500000.0
	ORDER BY id;

-- 3. Compound SPATIAL index with array containment prefix (`tags[*] CONTAINS 'express' AND loc INSIDE $uk_box`)
SELECT VALUE id FROM fleet
	WHERE tags[*] CONTAINS 'express' AND loc INSIDE $uk_box
	ORDER BY id;
SELECT VALUE id FROM fleet WITH NOINDEX
	WHERE tags[*] CONTAINS 'express' AND loc INSIDE $uk_box
	ORDER BY id;

-- 4. Multi-predicate spatial AND conjunction (`loc INSIDE $eur_box AND loc INSIDE $uk_box`)
SELECT VALUE id FROM fleet
	WHERE loc INSIDE $eur_box AND loc INSIDE $uk_box
	ORDER BY id;
SELECT VALUE id FROM fleet WITH NOINDEX
	WHERE loc INSIDE $eur_box AND loc INSIDE $uk_box
	ORDER BY id;

-- 5. Multi-predicate spatial AND (`loc INSIDE $eur_box AND geo::distance(loc, $london) < 20000.0`)
SELECT VALUE id FROM fleet
	WHERE loc INSIDE $eur_box AND geo::distance(loc, $london) < 20000.0
	ORDER BY id;
SELECT VALUE id FROM fleet WITH NOINDEX
	WHERE loc INSIDE $eur_box AND geo::distance(loc, $london) < 20000.0
	ORDER BY id;

-- 6. BitmapFusion: separate B-Tree index (`status = 'open'`) + SPATIAL index (`loc INSIDE $uk_box` and `geo::distance`)
SELECT VALUE id FROM fleet
	WHERE status = 'open' AND loc INSIDE $uk_box
	ORDER BY id;
SELECT VALUE id FROM fleet WITH NOINDEX
	WHERE status = 'open' AND loc INSIDE $uk_box
	ORDER BY id;

SELECT VALUE id FROM fleet
	WHERE status = 'open' AND geo::distance(loc, $london) < 500000.0
	ORDER BY id;
SELECT VALUE id FROM fleet WITH NOINDEX
	WHERE status = 'open' AND geo::distance(loc, $london) < 500000.0
	ORDER BY id;

-- 7. Disjunction (OR union) of single-column spatial predicates
SELECT VALUE id FROM fleet
	WHERE loc INSIDE $uk_box OR loc INSIDE $jp_box
	ORDER BY id;
SELECT VALUE id FROM fleet WITH NOINDEX
	WHERE loc INSIDE $uk_box OR loc INSIDE $jp_box
	ORDER BY id;

-- 8. Disjunction (OR union) of compound spatial predicates
SELECT VALUE id FROM fleet
	WHERE (tenant = 'acme' AND category = 'van' AND loc INSIDE $uk_box)
	   OR (tenant = 'globex' AND category = 'van' AND loc INSIDE $fr_box)
	ORDER BY id;
SELECT VALUE id FROM fleet WITH NOINDEX
	WHERE (tenant = 'acme' AND category = 'van' AND loc INSIDE $uk_box)
	   OR (tenant = 'globex' AND category = 'van' AND loc INSIDE $fr_box)
	ORDER BY id;

-- 9. Ordering (`ORDER BY id DESC`) and pagination (`LIMIT 2 START 1`)
SELECT VALUE id FROM fleet
	WHERE loc INSIDE $eur_box
	ORDER BY id DESC;
SELECT VALUE id FROM fleet WITH NOINDEX
	WHERE loc INSIDE $eur_box
	ORDER BY id DESC;

SELECT VALUE id FROM fleet
	WHERE loc INSIDE $eur_box
	ORDER BY id ASC LIMIT 2 START 1;
SELECT VALUE id FROM fleet WITH NOINDEX
	WHERE loc INSIDE $eur_box
	ORDER BY id ASC LIMIT 2 START 1;

-- 10. Hybrid HNSW vector KNN + single-column SPATIAL prefilter vs brute-force `<|2, EUCLIDEAN|>`
SELECT id, vector::distance::knn() AS d FROM fleet
	WHERE loc INSIDE $eur_box AND vec <|2, 40|> [1.0, 0.0]
	ORDER BY d, id;
SELECT id, vector::distance::knn() AS d FROM fleet
	WHERE loc INSIDE $eur_box AND vec <|2, EUCLIDEAN|> [1.0, 0.0]
	ORDER BY d, id;

-- 11. Hybrid HNSW vector KNN + compound SPATIAL prefilter vs brute-force `<|2, EUCLIDEAN|>`
SELECT id, vector::distance::knn() AS d FROM fleet
	WHERE tenant = 'acme' AND category = 'van' AND loc INSIDE $eur_box AND vec <|2, 40|> [1.0, 0.0]
	ORDER BY d, id;
SELECT id, vector::distance::knn() AS d FROM fleet
	WHERE tenant = 'acme' AND category = 'van' AND loc INSIDE $eur_box AND vec <|2, EUCLIDEAN|> [1.0, 0.0]
	ORDER BY d, id;

-- 12. Hybrid HNSW vector KNN + `geo::distance(loc, <point> $prior) < r` SPATIAL prefilter vs brute-force `<|2, EUCLIDEAN|>`
LET $prior = [-0.12, 51.51];
SELECT id, vector::distance::knn() AS d FROM fleet
	WHERE geo::distance(loc, <point> $prior) < 500000.0 AND vec <|2, 40|> [1.0, 0.0]
	ORDER BY d, id;
SELECT id, vector::distance::knn() AS d FROM fleet
	WHERE geo::distance(loc, <point> $prior) < 500000.0 AND vec <|2, EUCLIDEAN|> [1.0, 0.0]
	ORDER BY d, id;

-- 13. Dual-SPATIAL BitmapFusion: two distinct spatial columns (`pickup` and `dropoff`) intersected
DEFINE TABLE deliveries SCHEMAFULL;
DEFINE FIELD pickup  ON deliveries TYPE geometry<point>;
DEFINE FIELD dropoff ON deliveries TYPE geometry<point>;
DEFINE INDEX idx_deliv_pickup  ON deliveries FIELDS pickup SPATIAL;
DEFINE INDEX idx_deliv_dropoff ON deliveries FIELDS dropoff SPATIAL;

CREATE deliveries:d1 SET pickup = (-0.12, 51.51), dropoff = (2.35, 48.85) RETURN NONE;
CREATE deliveries:d2 SET pickup = (-0.12, 51.51), dropoff = (139.69, 35.68) RETURN NONE;
CREATE deliveries:d3 SET pickup = (139.69, 35.68), dropoff = (2.35, 48.85) RETURN NONE;

SELECT VALUE id FROM deliveries WHERE pickup INSIDE $uk_box AND dropoff INSIDE $fr_box;
SELECT VALUE id FROM deliveries WITH NOINDEX WHERE pickup INSIDE $uk_box AND dropoff INSIDE $fr_box;

-- 14. Unordered `LIMIT` and `LIMIT ... START ...` streaming pagination over a multi-branch spatial union
RETURN array::len(SELECT id FROM fleet WHERE loc INSIDE $eur_box LIMIT 3);
RETURN array::len(SELECT id FROM fleet WHERE loc INSIDE $eur_box LIMIT 2 START 2);
RETURN array::len(SELECT id FROM fleet WHERE loc INSIDE $eur_box LIMIT 4 START 3);

-- 15. EXPLAIN plan verification across index shapes (single-column SPATIAL, compound SPATIAL,
--     array-prefix SPATIAL, BitmapFusion, and KnnScan prefilter)
LET $e1 = (SELECT VALUE id FROM fleet WHERE loc INSIDE $london EXPLAIN);
RETURN [
	$e1.children[0].operator,
	$e1.children[0].attributes.branches,
	$e1.children[0].children[0].operator,
	$e1.children[0].children[0].attributes.index,
	$e1.children[0].children[0].attributes.point_filter
];
LET $e2 = (SELECT VALUE id FROM fleet WHERE tenant = 'acme' AND category = 'van' AND loc INSIDE $london EXPLAIN);
RETURN [
	$e2.children[0].operator,
	$e2.children[0].children[0].operator,
	$e2.children[0].children[0].attributes.branches,
	$e2.children[0].children[0].children[0].operator,
	$e2.children[0].children[0].children[0].children[0].attributes.index
];
LET $e3 = (SELECT VALUE id FROM fleet WHERE tags[*] CONTAINS 'express' AND loc INSIDE $london EXPLAIN);
RETURN [
	$e3.children[0].operator,
	$e3.children[0].children[0].operator,
	$e3.children[0].children[0].attributes.branches,
	$e3.children[0].children[0].children[0].operator,
	$e3.children[0].children[0].children[0].children[0].attributes.index
];
LET $e4 = (SELECT VALUE id FROM fleet WHERE status = 'open' AND loc INSIDE $london EXPLAIN);
RETURN [
	$e4.children[0].operator,
	$e4.children[0].children[0].operator,
	$e4.children[0].children[0].attributes.decode,
	$e4.children[0].children[0].children[0].operator,
	$e4.children[0].children[0].children[0].children[0].attributes.index,
	$e4.children[0].children[0].children[0].children[1].children[0].attributes.index,
	$e4.children[0].children[0].children[0].children[1].children[0].attributes.point_filter
];
LET $e5 = (SELECT id, vector::distance::knn() AS d FROM fleet WHERE loc INSIDE $london AND vec <|2, 40|> [1.0, 0.0] EXPLAIN);
RETURN [
	$e5.children[0].operator,
	$e5.children[0].children[0].operator,
	$e5.children[0].children[0].attributes.index,
	$e5.children[0].children[0].children[0].operator,
	$e5.children[0].children[0].children[0].children[0].attributes.index,
	$e5.children[0].children[0].children[0].children[0].attributes.point_filter
];
LET $e6 = (SELECT VALUE id FROM fleet WHERE loc INSIDE (-0.12, 51.51) OR loc INSIDE (139.69, 35.68) EXPLAIN);
RETURN [
	$e6.children[0].operator,
	$e6.children[0].children[0].operator,
	$e6.children[0].children[0].attributes.decode,
	$e6.children[0].children[0].children[0].operator,
	$e6.children[0].children[0].children[0].children[0].attributes.point_filter,
	$e6.children[0].children[0].children[0].children[1].attributes.point_filter
];

-- 16. Wide spatial covering (`$world_box`), globe-spanning zero-area line (`$eq_line`) combined
--     with a half-bounded B-tree range (`status > 'n'`), and a regional (~2.5%) box (`$reg_box`)
--     combined with `status = 'open'` fuse via `BitmapScan` and match `WITH NOINDEX`, and a
--     4-branch spatial `OR` union bounds per-branch coverings and matches `WITH NOINDEX`
LET $world_box = {
	type: "Polygon",
	coordinates: [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]
};
SELECT VALUE id FROM fleet WHERE status > 'n' AND loc INSIDE $world_box ORDER BY id;
SELECT VALUE id FROM fleet WITH NOINDEX WHERE status > 'n' AND loc INSIDE $world_box ORDER BY id;
LET $e7 = (SELECT VALUE id FROM fleet WHERE status > 'n' AND loc INSIDE $world_box EXPLAIN);
RETURN [
	$e7.children[0].operator,
	$e7.children[0].children[0].operator,
	$e7.children[0].children[0].attributes.index
];

LET $eq_line = {
	type: "LineString",
	coordinates: [[-179.0, 0.0], [179.0, 0.0]]
};
LET $e8 = (SELECT VALUE id FROM fleet WHERE status > 'n' AND loc INTERSECTS $eq_line EXPLAIN);
RETURN [
	$e8.children[0].operator,
	$e8.children[0].children[0].operator,
	$e8.children[0].children[0].attributes.index
];

LET $reg_box = {
	type: "Polygon",
	coordinates: [[[-15.0, 46.0], [20.0, 46.0], [20.0, 65.0], [-15.0, 65.0], [-15.0, 46.0]]]
};
SELECT VALUE id FROM fleet WHERE status = 'open' AND loc INSIDE $reg_box ORDER BY id;
SELECT VALUE id FROM fleet WITH NOINDEX WHERE status = 'open' AND loc INSIDE $reg_box ORDER BY id;
LET $e9 = (SELECT VALUE id FROM fleet WHERE status = 'open' AND loc INSIDE $reg_box EXPLAIN);
RETURN [
	$e9.children[0].operator,
	$e9.children[0].children[0].operator,
	$e9.children[0].children[0].attributes.decode,
	$e9.children[0].children[0].children[0].operator
];

SELECT VALUE id FROM fleet
	WHERE loc INSIDE $uk_box OR loc INSIDE $fr_box OR loc INSIDE $jp_box OR loc INSIDE $eur_box
	ORDER BY id;
SELECT VALUE id FROM fleet WITH NOINDEX
	WHERE loc INSIDE $uk_box OR loc INSIDE $fr_box OR loc INSIDE $jp_box OR loc INSIDE $eur_box
	ORDER BY id;


