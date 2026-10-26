/**
[env]

[test]
reason = "Differential Haversine distance (`geo::distance`) battery: direct and reversed function arguments, `<`, `<=`, `>`, `>=` comparison orientations, parameterized and arithmetic radius expressions, polar cap queries across the North Pole, antimeridian queries across +-180 longitude, and non-positive radii — each paired with its WITH NOINDEX ground-truth twin."

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
value = "[cities:amsterdam, cities:brussels, cities:london, cities:paris]"

[[test.results]]
value = "[cities:amsterdam, cities:brussels, cities:london, cities:paris]"

[[test.results]]
value = "[cities:amsterdam, cities:brussels, cities:london, cities:paris]"

[[test.results]]
value = "[cities:amsterdam, cities:brussels, cities:london, cities:paris]"

[[test.results]]
value = "[cities:amsterdam, cities:brussels, cities:london, cities:paris]"

[[test.results]]
value = "[cities:amsterdam, cities:brussels, cities:london, cities:paris]"

[[test.results]]
value = "[cities:amsterdam, cities:brussels, cities:london, cities:paris]"

[[test.results]]
value = "[cities:amsterdam, cities:brussels, cities:london, cities:paris]"

[[test.results]]
value = "[cities:amsterdam, cities:brussels, cities:london, cities:paris]"

[[test.results]]
value = "[cities:amsterdam, cities:brussels, cities:london, cities:paris]"

[[test.results]]
value = "[cities:amsterdam, cities:brussels, cities:london, cities:paris]"

[[test.results]]
value = "[cities:amsterdam, cities:brussels, cities:london, cities:paris]"

[[test.results]]
value = "[cities:london]"

[[test.results]]
value = "[cities:london]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[cities:pole_a, cities:pole_b]"

[[test.results]]
value = "[cities:pole_a, cities:pole_b]"

[[test.results]]
value = "[cities:dateline_e, cities:dateline_w]"

[[test.results]]
value = "[cities:dateline_e, cities:dateline_w]"

[[test.results]]
value = "[10]"

[[test.results]]
value = "[10]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[cities:wrapped]"

[[test.results]]
value = "[cities:wrapped]"

[[test.results]]
value = "[cities:wrapped]"

[[test.results]]
value = "[cities:wrapped]"

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
value = "[dense_pts:z_match]"

[[test.results]]
value = "[dense_pts:z_match]"

[[test.results]]
value = "[{ pt: (0f, 51.5f) }]"

[[test.results]]
value = "[{ pt: (0f, 51.5f) }]"

[[test.results]]
value = "[dense_pts:z_match]"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['UnionIndexScan', '2', 'IndexScan', 'idx_dense_pts', 'geo::distance(pt, (0, 51.5)) <= 0']"
*/

DEFINE TABLE cities SCHEMAFULL;
DEFINE FIELD pt ON cities TYPE geometry<point>;
DEFINE INDEX idx_cities_pt ON cities FIELDS pt SPATIAL;

CREATE cities:london     SET pt = (-0.1276, 51.5072) RETURN NONE;
CREATE cities:paris      SET pt = (2.3522, 48.8566) RETURN NONE;
CREATE cities:brussels   SET pt = (4.3517, 50.8503) RETURN NONE;
CREATE cities:amsterdam  SET pt = (4.9041, 52.3676) RETURN NONE;
CREATE cities:berlin     SET pt = (13.4050, 52.5200) RETURN NONE;
CREATE cities:tokyo      SET pt = (139.6917, 35.6895) RETURN NONE;
CREATE cities:pole_a     SET pt = (10.0, 89.6) RETURN NONE;
CREATE cities:pole_b     SET pt = (-170.0, 89.6) RETURN NONE;
CREATE cities:dateline_e SET pt = (179.85, 0.0) RETURN NONE;
CREATE cities:dateline_w SET pt = (-179.85, 0.0) RETURN NONE;

LET $london = (-0.1276, 51.5072);
LET $radius_400km = 400000.0;

-- 1. Direct `<` and `<=` with literal and parameterized origin/radius
SELECT VALUE id FROM cities WHERE geo::distance(pt, (-0.1276, 51.5072)) < 400000.0 ORDER BY id;
SELECT VALUE id FROM cities WITH NOINDEX WHERE geo::distance(pt, (-0.1276, 51.5072)) < 400000.0 ORDER BY id;

SELECT VALUE id FROM cities WHERE geo::distance(pt, $london) <= $radius_400km ORDER BY id;
SELECT VALUE id FROM cities WITH NOINDEX WHERE geo::distance(pt, $london) <= $radius_400km ORDER BY id;

-- 2. Reversed arguments inside `geo::distance($origin, pt)`
SELECT VALUE id FROM cities WHERE geo::distance($london, pt) < $radius_400km ORDER BY id;
SELECT VALUE id FROM cities WITH NOINDEX WHERE geo::distance($london, pt) < $radius_400km ORDER BY id;

-- 3. Reversed comparison operators (`$r > geo::distance(...)` and `$r >= geo::distance(...)`)
SELECT VALUE id FROM cities WHERE 400000.0 > geo::distance(pt, $london) ORDER BY id;
SELECT VALUE id FROM cities WITH NOINDEX WHERE 400000.0 > geo::distance(pt, $london) ORDER BY id;

SELECT VALUE id FROM cities WHERE $radius_400km >= geo::distance($london, pt) ORDER BY id;
SELECT VALUE id FROM cities WITH NOINDEX WHERE $radius_400km >= geo::distance($london, pt) ORDER BY id;

-- 4. Constant-folded arithmetic radius expression (`360 * 1000`)
SELECT VALUE id FROM cities WHERE geo::distance(pt, $london) < 360 * 1000 ORDER BY id;
SELECT VALUE id FROM cities WITH NOINDEX WHERE geo::distance(pt, $london) < 360 * 1000 ORDER BY id;

-- 5. Exact 0m distance (`<= 0.0`) matches the exact point
SELECT VALUE id FROM cities WHERE geo::distance(pt, $london) <= 0.0 ORDER BY id;
SELECT VALUE id FROM cities WITH NOINDEX WHERE geo::distance(pt, $london) <= 0.0 ORDER BY id;

-- 6. Strictly negative / impossible distance (`< 0.0`, `<= -10.0`) returns empty
SELECT VALUE id FROM cities WHERE geo::distance(pt, $london) < 0.0 ORDER BY id;
SELECT VALUE id FROM cities WITH NOINDEX WHERE geo::distance(pt, $london) < 0.0 ORDER BY id;

SELECT VALUE id FROM cities WHERE geo::distance(pt, $london) <= -10.0 ORDER BY id;
SELECT VALUE id FROM cities WITH NOINDEX WHERE geo::distance(pt, $london) <= -10.0 ORDER BY id;

-- 7. Polar cap query: within 150km of (10.0, 89.6) reaches across the North Pole to (-170.0, 89.6)
SELECT VALUE id FROM cities WHERE geo::distance(pt, (10.0, 89.6)) < 150000.0 ORDER BY id;
SELECT VALUE id FROM cities WITH NOINDEX WHERE geo::distance(pt, (10.0, 89.6)) < 150000.0 ORDER BY id;

-- 8. Antimeridian query: within 50km of (179.85, 0.0) reaches across +-180 longitude to (-179.85, 0.0)
SELECT VALUE id FROM cities WHERE geo::distance(pt, (179.85, 0.0)) < 50000.0 ORDER BY id;
SELECT VALUE id FROM cities WITH NOINDEX WHERE geo::distance(pt, (179.85, 0.0)) < 50000.0 ORDER BY id;

-- 9. Full-globe radius (>= pi * R_earth) matches all indexed points
SELECT VALUE count() FROM cities WHERE geo::distance(pt, $london) < 25000000.0 GROUP ALL;
SELECT VALUE count() FROM cities WITH NOINDEX WHERE geo::distance(pt, $london) < 25000000.0 GROUP ALL;

-- 10. Out-of-domain spherical coordinates (lon = 220.0 wraps to -140.0 in Haversine)
CREATE cities:wrapped SET pt = (220.0, 0.0) RETURN NONE;
SELECT VALUE id FROM cities WHERE geo::distance(pt, (-140.0, 0.0)) < 100.0 ORDER BY id;
SELECT VALUE id FROM cities WITH NOINDEX WHERE geo::distance(pt, (-140.0, 0.0)) < 100.0 ORDER BY id;

SELECT VALUE id FROM cities WHERE geo::distance(pt, (220.0, 0.0)) < 100.0 ORDER BY id;
SELECT VALUE id FROM cities WITH NOINDEX WHERE geo::distance(pt, (220.0, 0.0)) < 100.0 ORDER BY id;

-- 11. Multi-batch inline `SpatialPointFilter` loop (`EntryScan::next_batch` > 256 entries in one cell)
DEFINE TABLE dense_pts SCHEMAFULL;
DEFINE FIELD pt ON dense_pts TYPE geometry<point>;
DEFINE INDEX idx_dense_pts ON dense_pts FIELDS pt SPATIAL;
FOR $i IN 0..300 {
	CREATE type::record('dense_pts', $i) SET pt = (0.0000001, 51.5000001);
};
CREATE dense_pts:z_match SET pt = (0.0, 51.5) RETURN NONE;
SELECT VALUE id FROM dense_pts WHERE geo::distance(pt, (0.0, 51.5)) <= 0.0 ORDER BY id;
SELECT VALUE id FROM dense_pts WITH NOINDEX WHERE geo::distance(pt, (0.0, 51.5)) <= 0.0 ORDER BY id;

-- Full-row fetch (`IndexRecords`) when an entire 256-entry batch is rejected by `point_filter`
SELECT pt FROM dense_pts WHERE pt INSIDE {
	type: "Polygon",
	coordinates: [[[-0.00000001, 51.49999999], [0.00000001, 51.49999999], [0.00000001, 51.50000001], [-0.00000001, 51.50000001], [-0.00000001, 51.49999999]]]
} ORDER BY id;
SELECT pt FROM dense_pts WITH NOINDEX WHERE pt INSIDE {
	type: "Polygon",
	coordinates: [[[-0.00000001, 51.49999999], [0.00000001, 51.49999999], [0.00000001, 51.50000001], [-0.00000001, 51.50000001], [-0.00000001, 51.49999999]]]
} ORDER BY id;

-- Unordered `LIMIT 1` with `point_filter` rejecting 300 entries before `dense_pts:z_match`,
-- and unordered `LIMIT 3 START 2` on `cities`
SELECT VALUE id FROM dense_pts WHERE geo::distance(pt, (0.0, 51.5)) <= 0.0 LIMIT 1;
RETURN array::len(SELECT id FROM cities WHERE geo::distance(pt, $london) < 1200000.0 LIMIT 3 START 2);

-- EXPLAIN plan verification for `geo::distance` spherical cap scan (no `DistinctRecords` on point-only branches)
LET $e = (SELECT VALUE id FROM dense_pts WHERE geo::distance(pt, (0.0, 51.5)) <= 0.0 EXPLAIN);
RETURN [
	$e.children[0].operator,
	$e.children[0].attributes.branches,
	$e.children[0].children[0].operator,
	$e.children[0].children[0].attributes.index,
	$e.children[0].children[0].attributes.point_filter
];


