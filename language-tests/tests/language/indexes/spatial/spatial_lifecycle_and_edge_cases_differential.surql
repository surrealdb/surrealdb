/**
[env]
timeout = 30000

[test]
requires = ["scripting"]
reason = "Differential spatial index lifecycle and edge-case battery: retroactive index creation on existing data, UPDATE (moving geometry, non-spatial field update, NONE/NULL cycle), UPSERT, DELETE, REBUILD INDEX, empty GeometryCollection/MultiPolygon vacuous truth, and domain boundary coordinates (-180..180, -90..90) — each verified with and without the SPATIAL index."

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
value = "[assets:a1]"

[[test.results]]
value = "[assets:a2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[assets:a1]"

[[test.results]]
value = "[assets:a1]"

[[test.results]]
value = "[assets:a2]"

[[test.results]]
value = "[assets:a2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[assets:a1]"

[[test.results]]
value = "[assets:a1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[assets:a1, assets:a2]"

[[test.results]]
value = "[assets:a1, assets:a2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[assets:a1]"

[[test.results]]
value = "[assets:a1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[assets:a1]"

[[test.results]]
value = "[assets:a1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[assets:a2]"

[[test.results]]
value = "[assets:a2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[assets:a4]"

[[test.results]]
value = "[assets:a4]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[assets:a4]"

[[test.results]]
value = "[assets:a4]"

[[test.results]]
value = "[assets:a1]"

[[test.results]]
value = "[assets:a1]"

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
value = "[edge_geoms:empty_col, edge_geoms:empty_mp]"

[[test.results]]
value = "[edge_geoms:empty_col, edge_geoms:empty_mp]"

[[test.results]]
value = "[edge_geoms:empty_col, edge_geoms:empty_mp, edge_geoms:london_col]"

[[test.results]]
value = "[edge_geoms:empty_col, edge_geoms:empty_mp, edge_geoms:london_col]"

[[test.results]]
value = "[edge_geoms:empty_col, edge_geoms:sw_corner]"

[[test.results]]
value = "[edge_geoms:empty_col, edge_geoms:sw_corner]"

[[test.results]]
value = "[edge_geoms:empty_col, edge_geoms:ne_corner]"

[[test.results]]
value = "[edge_geoms:empty_col, edge_geoms:ne_corner]"

[[test.results]]
value = "[edge_geoms:empty_col, edge_geoms:origin]"

[[test.results]]
value = "[edge_geoms:empty_col, edge_geoms:origin]"

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
value = "[retro_shapes:poly, retro_shapes:pt]"

[[test.results]]
value = "[retro_shapes:poly, retro_shapes:pt]"

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
value = "[uniq_places:u1]"

[[test.results]]
value = "[uniq_places:u1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[geojson_rows:r1]"

[[test.results]]
value = "[geojson_rows:r1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[scalar_mixed:pt]"

[[test.results]]
value = "[scalar_mixed:pt]"

[[test.results]]
error = "Incorrect arguments for function geo::distance(). Argument 1 was the wrong type. Expected `geometry` but found `'not_a_geometry'`"

[[test.results]]
error = "Incorrect arguments for function geo::distance(). Argument 1 was the wrong type. Expected `geometry` but found `'not_a_geometry'`"

[[test.results]]
value = "[retro_shapes:poly]"

[[test.results]]
value = "[retro_shapes:poly]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[shape_shift:pt]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Filter', 'UnionIndexScan', '1', 'DistinctRecords', 'idx_shape_shift']"

[[test.results]]
value = "[]"

[[test.results]]
value = "[shape_shift:poly, shape_shift:pt]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Filter', 'UnionIndexScan', '17', 'DistinctRecords', 'idx_shape_shift']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[inside_shift:pt]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['UnionIndexScan', 'IndexScan', true]"

[[test.results]]
value = "[inside_shift:poly, inside_shift:pt]"

[[test.results]]
value = "[inside_shift:poly, inside_shift:pt]"

[[test.results]]
value = "[inside_shift:poly, inside_shift:pt]"

[[test.results]]
value = "[inside_shift:poly, inside_shift:pt]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Filter', 'UnionIndexScan', 'DistinctRecords']"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[edge_geoms:empty_col, edge_geoms:inf_line]"

[[test.results]]
value = "[edge_geoms:empty_col, edge_geoms:inf_line]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['WriteOperator', 'SelectProject', 'UnionIndexScan', '17', 'DistinctRecords']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

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
value = "[{ id: proj_sl:p1, label: 'x:London', score: 12 }]"

[[test.results]]
value = "[{ id: proj_sl:p1, label: 'x:London', score: 12 }]"

[[test.results]]
value = "[{ id: proj_sf:p1, label: 'x:London', score: 12 }]"

[[test.results]]
value = "[{ id: proj_sf:p1, label: 'x:London', score: 12 }]"

[[test.results]]
value = "[{ id: proj_sl:p1, lim: 10, loc: (-0.12f, 51.51f), meta: { city: 'London', tier: 2 }, name: 'x', org_ref: org:hq }]"

[[test.results]]
value = "[{ id: proj_sl:p1, lim: 10, loc: (-0.12f, 51.51f), meta: { city: 'London', tier: 2 }, name: 'x', org_ref: org:hq }]"

[[test.results]]
value = "[{ id: proj_sf:p1, lim: 10, loc: (-0.12f, 51.51f), meta: { city: 'London', tier: 2 }, name: 'x', org_ref: org:hq }]"

[[test.results]]
value = "[{ id: proj_sf:p1, lim: 10, loc: (-0.12f, 51.51f), meta: { city: 'London', tier: 2 }, name: 'x', org_ref: org:hq }]"

[[test.results]]
value = "[{ id: proj_sl:p1, org_title: 'HQ', targets: ['T1'] }]"

[[test.results]]
value = "[{ id: proj_sl:p1, org_title: 'HQ', targets: ['T1'] }]"

[[test.results]]
value = "[{ id: proj_sf:p1, org_title: 'HQ', targets: ['T1'] }]"

[[test.results]]
value = "[{ id: proj_sf:p1, org_title: 'HQ', targets: ['T1'] }]"

[[test.results]]
value = "[{ id: proj_sl:p1, org_ref: { id: org:hq, title: 'HQ' } }]"

[[test.results]]
value = "[{ id: proj_sl:p1, org_ref: { id: org:hq, title: 'HQ' } }]"

[[test.results]]
value = "[{ id: proj_sf:p1, org_ref: { id: org:hq, title: 'HQ' } }]"

[[test.results]]
value = "[{ id: proj_sf:p1, org_ref: { id: org:hq, title: 'HQ' } }]"

[[test.results]]
value = "[proj_sl:p1]"

[[test.results]]
value = "[proj_sl:p1]"

[[test.results]]
value = "[proj_sf:p1]"

[[test.results]]
value = "[proj_sf:p1]"

[[test.results]]
value = "[{ id: proj_sl:p1, small: [5] }]"

[[test.results]]
value = "[{ id: proj_sl:p1, small: [5] }]"

[[test.results]]
value = "[{ id: proj_sf:p1, small: [5] }]"

[[test.results]]
value = "[{ id: proj_sf:p1, small: [5] }]"

[[test.results]]
value = "[{ c: ['London'], id: proj_sl:p1, n: ['x'] }]"

[[test.results]]
value = "[{ c: ['London'], id: proj_sl:p1, n: ['x'] }]"

[[test.results]]
value = "[{ c: ['London'], id: proj_sf:p1, n: ['x'] }]"

[[test.results]]
value = "[{ c: ['London'], id: proj_sf:p1, n: ['x'] }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[proj_sl:p1]"

[[test.results]]
value = "[proj_sl:p1]"

[[test.results]]
value = "[proj_sf:p1]"

[[test.results]]
value = "[proj_sf:p1]"

[[test.results]]
value = "[{ id: proj_sl:p1, js_name: 'x' }]"

[[test.results]]
value = "[{ id: proj_sl:p1, js_name: 'x' }]"

[[test.results]]
value = "[{ id: proj_sf:p1, js_name: 'x' }]"

[[test.results]]
value = "[{ id: proj_sf:p1, js_name: 'x' }]"

[[test.results]]
value = "[{ id: proj_sl:p1, name: 'x' }]"

[[test.results]]
value = "[{ id: proj_sl:p1, name: 'x' }]"

[[test.results]]
value = "[{ id: proj_sf:p1, name: 'x' }]"

[[test.results]]
value = "[{ id: proj_sf:p1, name: 'x' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['UnionIndexScan', 'yes', NONE]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['SelectProject', 'UnionIndexScan', NONE, '[lim, meta, name]']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['SelectProject', 'Sort', 'UnionIndexScan', NONE, '[name]']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['SelectProject', 'UnionIndexScan', NONE, '[org_ref]']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['SelectProject', 'UnionIndexScan', NONE, NONE]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Filter', 'UnionIndexScan', NONE, NONE]"

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
value = "[nf_geoms:in_inf_tri]"

[[test.results]]
value = "[nf_geoms:in_inf_tri]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Filter', 'UnionIndexScan', '1', '[loc]', 'UnionIndexScan', 'yes', 'IndexScan']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[nf_geoms:stray_hole]"

[[test.results]]
value = "[nf_geoms:stray_hole]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[nf_geoms:stray_hole, nf_geoms:stray_line]"

[[test.results]]
value = "[nf_geoms:stray_hole, nf_geoms:stray_line]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[nf_geoms:coll_poly_pt, nf_geoms:coll_pt]"

[[test.results]]
value = "[nf_geoms:coll_poly_pt, nf_geoms:coll_pt]"

[[test.results]]
value = "[nf_geoms:coll_poly_pt, nf_geoms:coll_pt]"

[[test.results]]
value = "[nf_geoms:coll_poly_pt, nf_geoms:coll_pt]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Filter', 'BitmapScan', 'TableScan']"

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
value = "[nullish_mixed:pt]"

[[test.results]]
value = "[nullish_mixed:pt]"

[[test.results]]
error = "Incorrect arguments for function geo::distance(). Argument 1 was the wrong type. Expected `geometry` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function geo::distance(). Argument 1 was the wrong type. Expected `geometry` but found `NONE`"
*/

LET $uk_box = {
	type: "Polygon",
	coordinates: [[[-2.0, 51.0], [1.0, 51.0], [1.0, 52.5], [-2.0, 52.5], [-2.0, 51.0]]]
};
LET $fr_box = {
	type: "Polygon",
	coordinates: [[[1.5, 48.0], [3.5, 48.0], [3.5, 49.5], [1.5, 49.5], [1.5, 48.0]]]
};

-- 1. Populate records BEFORE defining the SPATIAL index
DEFINE TABLE assets;
CREATE assets:a1 SET label = 'alpha', loc = (-0.12, 51.51) RETURN NONE;
CREATE assets:a2 SET label = 'beta',  loc = (2.35, 48.85) RETURN NONE;
CREATE assets:a3 SET label = 'gamma', loc = (139.69, 35.68) RETURN NONE;

-- Query BEFORE `DEFINE INDEX` (unindexed baseline)
SELECT VALUE id FROM assets WHERE loc INSIDE $uk_box ORDER BY id;
SELECT VALUE id FROM assets WHERE geo::distance(loc, (2.35, 48.85)) < 50000.0 ORDER BY id;

-- Define SPATIAL index retroactively and verify identical results
DEFINE INDEX idx_assets_loc ON assets FIELDS loc SPATIAL;

SELECT VALUE id FROM assets WHERE loc INSIDE $uk_box ORDER BY id;
SELECT VALUE id FROM assets WITH NOINDEX WHERE loc INSIDE $uk_box ORDER BY id;

SELECT VALUE id FROM assets WHERE geo::distance(loc, (2.35, 48.85)) < 50000.0 ORDER BY id;
SELECT VALUE id FROM assets WITH NOINDEX WHERE geo::distance(loc, (2.35, 48.85)) < 50000.0 ORDER BY id;

-- 2. UPDATE non-spatial field (`label`) preserves spatial index entry
UPDATE assets:a1 SET label = 'alpha_v2' RETURN NONE;
SELECT VALUE id FROM assets WHERE loc INSIDE $uk_box ORDER BY id;
SELECT VALUE id FROM assets WITH NOINDEX WHERE loc INSIDE $uk_box ORDER BY id;

-- 3. UPDATE spatial field: move `assets:a1` from UK to France
UPDATE assets:a1 SET loc = (2.40, 48.90) RETURN NONE;
SELECT VALUE id FROM assets WHERE loc INSIDE $uk_box ORDER BY id;
SELECT VALUE id FROM assets WITH NOINDEX WHERE loc INSIDE $uk_box ORDER BY id;

SELECT VALUE id FROM assets WHERE loc INSIDE $fr_box ORDER BY id;
SELECT VALUE id FROM assets WITH NOINDEX WHERE loc INSIDE $fr_box ORDER BY id;

-- 4. UPDATE spatial field through NONE -> NULL -> valid geometry cycle
UPDATE assets:a2 SET loc = NONE RETURN NONE;
SELECT VALUE id FROM assets WHERE loc INSIDE $fr_box ORDER BY id;
SELECT VALUE id FROM assets WITH NOINDEX WHERE loc INSIDE $fr_box ORDER BY id;

UPDATE assets:a2 SET loc = NULL RETURN NONE;
SELECT VALUE id FROM assets WHERE loc INSIDE $fr_box ORDER BY id;
SELECT VALUE id FROM assets WITH NOINDEX WHERE loc INSIDE $fr_box ORDER BY id;

UPDATE assets:a2 SET loc = (-0.12, 51.51) RETURN NONE;
SELECT VALUE id FROM assets WHERE loc INSIDE $uk_box ORDER BY id;
SELECT VALUE id FROM assets WITH NOINDEX WHERE loc INSIDE $uk_box ORDER BY id;

-- 5. UPSERT and DELETE lifecycle
UPSERT assets:a4 SET label = 'delta', loc = (-0.50, 51.60) RETURN NONE;
DELETE assets:a2 RETURN NONE;

SELECT VALUE id FROM assets WHERE loc INSIDE $uk_box ORDER BY id;
SELECT VALUE id FROM assets WITH NOINDEX WHERE loc INSIDE $uk_box ORDER BY id;

-- 6. REBUILD INDEX preserves exact query parity
REBUILD INDEX idx_assets_loc ON assets;
SELECT VALUE id FROM assets WHERE loc INSIDE $uk_box ORDER BY id;
SELECT VALUE id FROM assets WITH NOINDEX WHERE loc INSIDE $uk_box ORDER BY id;

SELECT VALUE id FROM assets WITH INDEX idx_assets_loc WHERE loc INSIDE $fr_box ORDER BY id;
SELECT VALUE id FROM assets WITH NOINDEX WHERE loc INSIDE $fr_box ORDER BY id;

-- 7. Empty geometries and domain boundary coordinates (-180..180, -90..90)
DEFINE TABLE edge_geoms;
DEFINE INDEX idx_edge_geoms ON edge_geoms FIELDS g SPATIAL;

CREATE edge_geoms:origin     SET g = (0.0, 0.0) RETURN NONE;
CREATE edge_geoms:sw_corner  SET g = (-180.0, -90.0) RETURN NONE;
CREATE edge_geoms:ne_corner  SET g = (180.0, 90.0) RETURN NONE;
CREATE edge_geoms:empty_col  SET g = { type: "GeometryCollection", geometries: [] } RETURN NONE;
CREATE edge_geoms:empty_mp   SET g = { type: "MultiPolygon", coordinates: [] } RETURN NONE;
CREATE edge_geoms:london_col SET g = {
	type: "GeometryCollection",
	geometries: [{ type: "Point", coordinates: [-0.12, 51.51] }]
} RETURN NONE;

-- `g INSIDE $fr_box` matches `empty_col` and `empty_mp` vacuously, with and without the index
SELECT VALUE id FROM edge_geoms WHERE g INSIDE $fr_box ORDER BY id;
SELECT VALUE id FROM edge_geoms WITH NOINDEX WHERE g INSIDE $fr_box ORDER BY id;

-- `g INSIDE $uk_box` matches `empty_col`, `empty_mp`, and `london_col`, with and without the index
SELECT VALUE id FROM edge_geoms WHERE g INSIDE $uk_box ORDER BY id;
SELECT VALUE id FROM edge_geoms WITH NOINDEX WHERE g INSIDE $uk_box ORDER BY id;

-- Boundary coordinate queries at (-180, -90), (180, 90), and (0, 0)
SELECT VALUE id FROM edge_geoms WHERE g INTERSECTS (-180.0, -90.0) ORDER BY id;
SELECT VALUE id FROM edge_geoms WITH NOINDEX WHERE g INTERSECTS (-180.0, -90.0) ORDER BY id;

SELECT VALUE id FROM edge_geoms WHERE g INTERSECTS (180.0, 90.0) ORDER BY id;
SELECT VALUE id FROM edge_geoms WITH NOINDEX WHERE g INTERSECTS (180.0, 90.0) ORDER BY id;

SELECT VALUE id FROM edge_geoms WHERE g INTERSECTS (0.0, 0.0) ORDER BY id;
SELECT VALUE id FROM edge_geoms WITH NOINDEX WHERE g INTERSECTS (0.0, 0.0) ORDER BY id;

-- 8. Retroactive `DEFINE FIELD ... TYPE geometry<point>` after indexing a Polygon record
--    must not skip coarse ancestor probes (`IndexShapes` records `ValueShape::Array`).
DEFINE TABLE retro_shapes;
DEFINE INDEX idx_retro_g ON retro_shapes FIELDS g SPATIAL;
CREATE retro_shapes:poly SET g = $uk_box RETURN NONE;
CREATE retro_shapes:pt   SET g = (-0.12, 51.51) RETURN NONE;
DEFINE FIELD g ON retro_shapes TYPE geometry<point>;

SELECT VALUE id FROM retro_shapes WHERE g CONTAINS (-0.12, 51.51) ORDER BY id;
SELECT VALUE id FROM retro_shapes WITH NOINDEX WHERE g CONTAINS (-0.12, 51.51) ORDER BY id;

-- 9. Unique index + spatial predicate conjunction
DEFINE TABLE uniq_places;
DEFINE INDEX idx_uniq_code ON uniq_places FIELDS code UNIQUE;
DEFINE INDEX idx_uniq_loc  ON uniq_places FIELDS loc SPATIAL;
CREATE uniq_places:u1 SET code = 'LON', loc = (-0.12, 51.51) RETURN NONE;
CREATE uniq_places:u2 SET code = 'PAR', loc = (2.35, 48.85) RETURN NONE;

SELECT VALUE id FROM uniq_places WHERE code = 'LON' AND loc INSIDE $uk_box ORDER BY id;
SELECT VALUE id FROM uniq_places WITH NOINDEX WHERE code = 'LON' AND loc INSIDE $uk_box ORDER BY id;

-- 10. Uncoerced GeoJSON Object stored on a schemaless table is indexed via `Value::geometry_operand()`
DEFINE TABLE geojson_rows;
DEFINE INDEX idx_geojson_loc ON geojson_rows FIELDS loc SPATIAL;
CREATE geojson_rows:r1 SET loc = encoding::json::decode('{"type":"Point","coordinates":[-0.12,51.51]}') RETURN NONE;
CREATE geojson_rows:r2 SET loc = encoding::json::decode('{"type":"Point","coordinates":[139.69,35.68]}') RETURN NONE;

SELECT VALUE id FROM geojson_rows WHERE loc INSIDE $uk_box ORDER BY id;
SELECT VALUE id FROM geojson_rows WITH NOINDEX WHERE loc INSIDE $uk_box ORDER BY id;

-- 11. Non-geometry scalar value on a schemaless table keeps `point_only_spatial` for `INSIDE`
--     while recording `ValueShape::NonGeometry` so `geo::distance` declines index pushdown and
--     raises the same type error as `WITH NOINDEX`
DEFINE TABLE scalar_mixed;
DEFINE INDEX idx_scalar_mixed ON scalar_mixed FIELDS loc SPATIAL;
CREATE scalar_mixed:str SET loc = 'not_a_geometry' RETURN NONE;
CREATE scalar_mixed:pt  SET loc = (-0.12, 51.51) RETURN NONE;

SELECT VALUE id FROM scalar_mixed WHERE loc INSIDE $uk_box ORDER BY id;
SELECT VALUE id FROM scalar_mixed WITH NOINDEX WHERE loc INSIDE $uk_box ORDER BY id;
SELECT VALUE id FROM scalar_mixed WHERE geo::distance(loc, (-0.12, 51.51)) < 50000.0 ORDER BY id;
SELECT VALUE id FROM scalar_mixed WITH NOINDEX WHERE geo::distance(loc, (-0.12, 51.51)) < 50000.0 ORDER BY id;

-- 12. `geo::distance` on a table containing a Polygon record (`retro_shapes:poly`) declines cap
--     expansion and matches `WITH NOINDEX` (`geo::distance(Polygon, Point)` is `NONE`, and `NONE < r` is `true`)
SELECT VALUE id FROM retro_shapes WHERE geo::distance(g, (139.69, 35.68)) < 1000.0 ORDER BY id;
SELECT VALUE id FROM retro_shapes WITH NOINDEX WHERE geo::distance(g, (139.69, 35.68)) < 1000.0 ORDER BY id;

-- 13. Plan reuse after a shape change: a table initially populated only with Point records
--     transitions `TableIndexShapes` from point-only to `MultiCell` when a Polygon is inserted,
--     and subsequent invocations of a defined function (`fn::find_at`) and direct queries
--     probe coarse ancestor cells and return both the Polygon and Point records.
DEFINE TABLE shape_shift;
DEFINE INDEX idx_shape_shift ON shape_shift FIELDS g SPATIAL;
CREATE shape_shift:pt SET g = (-0.12, 51.51) RETURN NONE;
DEFINE FUNCTION fn::find_at($pt: geometry) {
	RETURN SELECT VALUE id FROM shape_shift WHERE g CONTAINS $pt ORDER BY id;
};

RETURN fn::find_at((-0.12, 51.51));
LET $e_shift1 = (SELECT VALUE id FROM shape_shift WHERE g CONTAINS (-0.12, 51.51) EXPLAIN);
RETURN [
	$e_shift1.children[0].operator,
	$e_shift1.children[0].children[0].operator,
	$e_shift1.children[0].children[0].attributes.branches,
	$e_shift1.children[0].children[0].children[0].operator,
	$e_shift1.children[0].children[0].children[0].children[0].attributes.index
];

CREATE shape_shift:poly SET g = $uk_box RETURN NONE;
RETURN fn::find_at((-0.12, 51.51));
LET $e_shift2 = (SELECT VALUE id FROM shape_shift WHERE g CONTAINS (-0.12, 51.51) EXPLAIN);
RETURN [
	$e_shift2.children[0].operator,
	$e_shift2.children[0].children[0].operator,
	$e_shift2.children[0].children[0].attributes.branches,
	$e_shift2.children[0].children[0].children[0].operator,
	$e_shift2.children[0].children[0].children[0].children[0].attributes.index
];

-- 14. Eliminated-filter reuse (`INSIDE` variant): a point-only table initially plans
--     `UnionIndexScan` with `point_filter` and no `Filter`; after a Polygon inside `$uk_box`
--     is inserted (inside a function transaction and seen by subsequent function and direct
--     queries), `TableIndexShapes` transitions to `MultiCell` and retains `Filter`.
DEFINE TABLE inside_shift;
DEFINE INDEX idx_inside_shift ON inside_shift FIELDS g SPATIAL;
CREATE inside_shift:pt SET g = (-0.12, 51.51) RETURN NONE;
DEFINE FUNCTION fn::find_inside($box: geometry) {
	RETURN SELECT VALUE id FROM inside_shift WHERE g INSIDE $box ORDER BY id;
};
DEFINE FUNCTION fn::insert_poly_and_find($box: geometry) {
	CREATE inside_shift:poly SET g = {
		type: "Polygon",
		coordinates: [[[-0.5, 51.2], [0.2, 51.2], [0.2, 51.8], [-0.5, 51.8], [-0.5, 51.2]]]
	} RETURN NONE;
	RETURN SELECT VALUE id FROM inside_shift WHERE g INSIDE $box ORDER BY id;
};

RETURN fn::find_inside($uk_box);
LET $e_in1 = (SELECT VALUE id FROM inside_shift WHERE g INSIDE $uk_box EXPLAIN);
RETURN [
	$e_in1.children[0].operator,
	$e_in1.children[0].children[0].operator,
	$e_in1.children[0].children[0].attributes.point_filter != NONE
];
RETURN fn::insert_poly_and_find($uk_box);
RETURN fn::find_inside($uk_box);
SELECT VALUE id FROM inside_shift WHERE g INSIDE $uk_box ORDER BY id;
SELECT VALUE id FROM inside_shift WITH NOINDEX WHERE g INSIDE $uk_box ORDER BY id;
LET $e_in2 = (SELECT VALUE id FROM inside_shift WHERE g INSIDE $uk_box EXPLAIN);
RETURN [
	$e_in2.children[0].operator,
	$e_in2.children[0].children[0].operator,
	$e_in2.children[0].children[0].children[0].operator
];

-- 15. Stored LineString with an infinite coordinate (`[math::inf, 0.0]`) is indexed at the coarse
--     root sentinel cell and matches `WITH NOINDEX`
CREATE edge_geoms:inf_line SET g = { type: "LineString", coordinates: [[0.0, 0.0], [math::inf, 0.0]] } RETURN NONE;
LET $east_box = {
	type: "Polygon",
	coordinates: [[[99.0, -1.0], [101.0, -1.0], [101.0, 1.0], [99.0, 1.0], [99.0, -1.0]]]
};
SELECT VALUE id FROM edge_geoms WHERE g INTERSECTS $east_box ORDER BY id;
SELECT VALUE id FROM edge_geoms WITH NOINDEX WHERE g INTERSECTS $east_box ORDER BY id;

-- 16. Write statement (`UPDATE ... WHERE <spatial>`) retains conservative multi-cell covering
LET $e_upd = (UPDATE assets SET label = label WHERE loc INSIDE (-0.12, 51.51) EXPLAIN);
RETURN [
	$e_upd.operator,
	$e_upd.children[0].operator,
	$e_upd.children[0].children[0].operator,
	$e_upd.children[0].children[0].attributes.branches,
	$e_upd.children[0].children[0].children[0].operator
];

-- 17. Projected decode (`DecodeFields`) and implicit-document-read bailouts (closures, `$parent`,
--     `$this`, scripts, `OMIT`, `FETCH`, record links, graph traversal, nested objects) on both
--     schemaless and `SCHEMAFULL` (packed) tables compared against `WITH NOINDEX`
DEFINE TABLE org SCHEMAFULL;
DEFINE FIELD title ON org TYPE string;
CREATE org:hq SET title = 'HQ' RETURN NONE;

DEFINE TABLE target_node SCHEMAFULL;
DEFINE FIELD code ON target_node TYPE string;
CREATE target_node:t1 SET code = 'T1' RETURN NONE;

DEFINE TABLE proj_sl;
DEFINE INDEX idx_proj_sl_loc ON proj_sl FIELDS loc SPATIAL;
CREATE proj_sl:p1 SET
	loc = (-0.12, 51.51),
	name = 'x',
	tags = ['x', 'y'],
	lim = 10,
	vals = [5, 20],
	secret = 'hidden',
	org_ref = org:hq,
	meta = { city: 'London', tier: 2 }
	RETURN NONE;
RELATE proj_sl:p1->linked_to->target_node:t1 RETURN NONE;

DEFINE TABLE proj_sf SCHEMAFULL;
DEFINE FIELD loc     ON proj_sf TYPE geometry<point>;
DEFINE FIELD name    ON proj_sf TYPE string;
DEFINE FIELD tags    ON proj_sf TYPE array<string>;
DEFINE FIELD lim     ON proj_sf TYPE int;
DEFINE FIELD vals    ON proj_sf TYPE array<int>;
DEFINE FIELD secret  ON proj_sf TYPE string;
DEFINE FIELD org_ref ON proj_sf TYPE record<org>;
DEFINE FIELD meta    ON proj_sf TYPE object;
DEFINE FIELD meta.city ON proj_sf TYPE string;
DEFINE FIELD meta.tier ON proj_sf TYPE int;
DEFINE INDEX idx_proj_sf_loc ON proj_sf FIELDS loc SPATIAL;
CREATE proj_sf:p1 SET
	loc = (-0.12, 51.51),
	name = 'x',
	tags = ['x', 'y'],
	lim = 10,
	vals = [5, 20],
	secret = 'hidden',
	org_ref = org:hq,
	meta = { city: 'London', tier: 2 }
	RETURN NONE;
RELATE proj_sf:p1->linked_to->target_node:t1 RETURN NONE;

-- Projected decode of computed expressions over top-level and nested fields (no closure/script)
SELECT id, string::concat(name, ':', meta.city) AS label, meta.tier + lim AS score FROM proj_sl WHERE loc INSIDE $uk_box;
SELECT id, string::concat(name, ':', meta.city) AS label, meta.tier + lim AS score FROM proj_sl WITH NOINDEX WHERE loc INSIDE $uk_box;
SELECT id, string::concat(name, ':', meta.city) AS label, meta.tier + lim AS score FROM proj_sf WHERE loc INSIDE $uk_box;
SELECT id, string::concat(name, ':', meta.city) AS label, meta.tier + lim AS score FROM proj_sf WITH NOINDEX WHERE loc INSIDE $uk_box;

-- `OMIT` on schemaless and `SCHEMAFULL` tables
SELECT * OMIT secret, vals, tags FROM proj_sl WHERE loc INSIDE $uk_box;
SELECT * OMIT secret, vals, tags FROM proj_sl WITH NOINDEX WHERE loc INSIDE $uk_box;
SELECT * OMIT secret, vals, tags FROM proj_sf WHERE loc INSIDE $uk_box;
SELECT * OMIT secret, vals, tags FROM proj_sf WITH NOINDEX WHERE loc INSIDE $uk_box;

-- Record-link traversal (`org_ref.title`), `FETCH`, and graph traversal (`->linked_to->target_node.code`)
SELECT id, org_ref.title AS org_title, ->linked_to->target_node.code AS targets FROM proj_sl WHERE loc INSIDE $uk_box;
SELECT id, org_ref.title AS org_title, ->linked_to->target_node.code AS targets FROM proj_sl WITH NOINDEX WHERE loc INSIDE $uk_box;
SELECT id, org_ref.title AS org_title, ->linked_to->target_node.code AS targets FROM proj_sf WHERE loc INSIDE $uk_box;
SELECT id, org_ref.title AS org_title, ->linked_to->target_node.code AS targets FROM proj_sf WITH NOINDEX WHERE loc INSIDE $uk_box;

SELECT id, org_ref FROM proj_sl WHERE loc INSIDE $uk_box FETCH org_ref;
SELECT id, org_ref FROM proj_sl WITH NOINDEX WHERE loc INSIDE $uk_box FETCH org_ref;
SELECT id, org_ref FROM proj_sf WHERE loc INSIDE $uk_box FETCH org_ref;
SELECT id, org_ref FROM proj_sf WITH NOINDEX WHERE loc INSIDE $uk_box FETCH org_ref;

-- Closures reading unprojected fields (`name`, `lim`), `$parent`, `$this`, and `count()` with closure filter
SELECT VALUE id FROM proj_sl WHERE loc INSIDE $uk_box AND array::any(tags, |$t| $t = name);
SELECT VALUE id FROM proj_sl WITH NOINDEX WHERE loc INSIDE $uk_box AND array::any(tags, |$t| $t = name);
SELECT VALUE id FROM proj_sf WHERE loc INSIDE $uk_box AND array::any(tags, |$t| $t = name);
SELECT VALUE id FROM proj_sf WITH NOINDEX WHERE loc INSIDE $uk_box AND array::any(tags, |$t| $t = name);

SELECT id, array::filter(vals, |$v| $v < lim) AS small FROM proj_sl WHERE loc INSIDE $uk_box;
SELECT id, array::filter(vals, |$v| $v < lim) AS small FROM proj_sl WITH NOINDEX WHERE loc INSIDE $uk_box;
SELECT id, array::filter(vals, |$v| $v < lim) AS small FROM proj_sf WHERE loc INSIDE $uk_box;
SELECT id, array::filter(vals, |$v| $v < lim) AS small FROM proj_sf WITH NOINDEX WHERE loc INSIDE $uk_box;

SELECT id, array::map([1], |$x| $parent.name) AS n, array::map([1], |$x| $this.meta.city) AS c FROM proj_sl WHERE loc INSIDE $uk_box;
SELECT id, array::map([1], |$x| $parent.name) AS n, array::map([1], |$x| $this.meta.city) AS c FROM proj_sl WITH NOINDEX WHERE loc INSIDE $uk_box;
SELECT id, array::map([1], |$x| $parent.name) AS n, array::map([1], |$x| $this.meta.city) AS c FROM proj_sf WHERE loc INSIDE $uk_box;
SELECT id, array::map([1], |$x| $parent.name) AS n, array::map([1], |$x| $this.meta.city) AS c FROM proj_sf WITH NOINDEX WHERE loc INSIDE $uk_box;

SELECT count() FROM proj_sl WHERE loc INSIDE $uk_box AND array::any(tags, |$t| $t = name) GROUP ALL;
SELECT count() FROM proj_sl WITH NOINDEX WHERE loc INSIDE $uk_box AND array::any(tags, |$t| $t = name) GROUP ALL;
SELECT count() FROM proj_sf WHERE loc INSIDE $uk_box AND array::any(tags, |$t| $t = name) GROUP ALL;
SELECT count() FROM proj_sf WITH NOINDEX WHERE loc INSIDE $uk_box AND array::any(tags, |$t| $t = name) GROUP ALL;

-- Closure under `geo::distance` predicate and embedded script (`function() { return this.name; }`)
SELECT VALUE id FROM proj_sl WHERE geo::distance(loc, (-0.12, 51.51)) < 50000.0 AND array::any(tags, |$t| $t = name);
SELECT VALUE id FROM proj_sl WITH NOINDEX WHERE geo::distance(loc, (-0.12, 51.51)) < 50000.0 AND array::any(tags, |$t| $t = name);
SELECT VALUE id FROM proj_sf WHERE geo::distance(loc, (-0.12, 51.51)) < 50000.0 AND array::any(tags, |$t| $t = name);
SELECT VALUE id FROM proj_sf WITH NOINDEX WHERE geo::distance(loc, (-0.12, 51.51)) < 50000.0 AND array::any(tags, |$t| $t = name);

SELECT id, function() { return this.name; } AS js_name FROM proj_sl WHERE loc INSIDE $uk_box;
SELECT id, function() { return this.name; } AS js_name FROM proj_sl WITH NOINDEX WHERE loc INSIDE $uk_box;
SELECT id, function() { return this.name; } AS js_name FROM proj_sf WHERE loc INSIDE $uk_box;
SELECT id, function() { return this.name; } AS js_name FROM proj_sf WITH NOINDEX WHERE loc INSIDE $uk_box;

-- Projected `ORDER BY` (`reader_runs` across `PhysOp::Pipeline`) on schemaless and `SCHEMAFULL` tables
SELECT id, name FROM proj_sl WHERE loc INSIDE $uk_box ORDER BY name;
SELECT id, name FROM proj_sl WITH NOINDEX WHERE loc INSIDE $uk_box ORDER BY name;
SELECT id, name FROM proj_sf WHERE loc INSIDE $uk_box ORDER BY name;
SELECT id, name FROM proj_sf WITH NOINDEX WHERE loc INSIDE $uk_box ORDER BY name;

-- Structured `EXPLAIN` verification of `id_only` / `decode` on `UnionIndexScan`:
-- 1) `SELECT VALUE id` under an eliminated `point_filter` emits `id_only: 'yes'`
LET $e_dec_id = (SELECT VALUE id FROM proj_sf WHERE loc INSIDE $uk_box EXPLAIN);
RETURN [
	$e_dec_id.children[0].operator,
	$e_dec_id.children[0].attributes.id_only,
	$e_dec_id.children[0].attributes.decode
];
-- 2) Projected computed expression emits `decode: '[lim, meta, name]'`
LET $e_dec_proj = (SELECT id, string::concat(name, ':', meta.city) AS label, meta.tier + lim AS score FROM proj_sf WHERE loc INSIDE $uk_box EXPLAIN);
RETURN [
	$e_dec_proj.operator,
	$e_dec_proj.children[0].operator,
	$e_dec_proj.children[0].attributes.id_only,
	$e_dec_proj.children[0].attributes.decode
];
-- 3) `ORDER BY name` + `SELECT id, name` emits `decode: '[name]'`
LET $e_dec_sort = (SELECT id, name FROM proj_sf WHERE loc INSIDE $uk_box ORDER BY name EXPLAIN);
RETURN [
	$e_dec_sort.operator,
	$e_dec_sort.children[0].operator,
	$e_dec_sort.children[0].children[0].operator,
	$e_dec_sort.children[0].children[0].attributes.id_only,
	$e_dec_sort.children[0].children[0].attributes.decode
];
-- 4) Record-link deref (`org_ref.title` with `deref_windows`) emits `decode: '[org_ref]'`
LET $e_dec_deref = (SELECT id, org_ref.title AS org_title FROM proj_sf WHERE loc INSIDE $uk_box EXPLAIN);
RETURN [
	$e_dec_deref.operator,
	$e_dec_deref.children[0].operator,
	$e_dec_deref.children[0].attributes.id_only,
	$e_dec_deref.children[0].attributes.decode
];
-- 5) `OMIT` falls back to whole-record decode (`id_only == NONE` and `decode == NONE`)
LET $e_dec_omit = (SELECT * OMIT secret, vals, tags FROM proj_sf WHERE loc INSIDE $uk_box EXPLAIN);
RETURN [
	$e_dec_omit.operator,
	$e_dec_omit.children[0].operator,
	$e_dec_omit.children[0].attributes.id_only,
	$e_dec_omit.children[0].attributes.decode
];
-- 6) Closure reading an unprojected field (`name`) falls back to whole-record decode
LET $e_dec_clos = (SELECT VALUE id FROM proj_sf WHERE loc INSIDE $uk_box AND array::any(tags, |$t| $t = name) EXPLAIN);
RETURN [
	$e_dec_clos.children[0].operator,
	$e_dec_clos.children[0].children[0].operator,
	$e_dec_clos.children[0].children[0].attributes.id_only,
	$e_dec_clos.children[0].children[0].attributes.decode
];

-- 18. Non-finite query polygon (`[math::inf, 25.0]`) declines spatial index expansion and matches
--     `WITH NOINDEX`, and GeoJSON `Point` object table (`geojson_rows`) records `ValueShape::Whole`
--     so `CONTAINS` plans 1 branch (`decode: '[id, loc]'`) and `INSIDE` plans `id_only: 'yes'`
--     without `DistinctRecords`
DEFINE TABLE nf_geoms;
DEFINE INDEX idx_nf_geoms ON nf_geoms FIELDS g SPATIAL;
CREATE nf_geoms:in_inf_tri SET g = (120.0, 25.0) RETURN NONE;
CREATE nf_geoms:out_pt     SET g = (-50.0, -50.0) RETURN NONE;
LET $inf_tri = {
	type: "Polygon",
	coordinates: [[[0.0, 0.0], [50.0, 0.0], [math::inf, 25.0], [50.0, 50.0], [0.0, 50.0], [0.0, 0.0]]]
};
SELECT VALUE id FROM nf_geoms WHERE g INSIDE $inf_tri ORDER BY id;
SELECT VALUE id FROM nf_geoms WITH NOINDEX WHERE g INSIDE $inf_tri ORDER BY id;

LET $e_gj = (SELECT VALUE id FROM geojson_rows WHERE loc CONTAINS (-0.12, 51.51) EXPLAIN);
LET $e_gj_in = (SELECT VALUE id FROM geojson_rows WHERE loc INSIDE $uk_box EXPLAIN);
RETURN [
	$e_gj.children[0].operator,
	$e_gj.children[0].children[0].operator,
	$e_gj.children[0].children[0].attributes.branches,
	$e_gj.children[0].children[0].attributes.decode,
	$e_gj_in.children[0].operator,
	$e_gj_in.children[0].attributes.id_only,
	$e_gj_in.children[0].children[0].operator
];

-- 19. `Polygon` with an interior ring outside its exterior bbox (`stray_hole`, both directions) and
--     `GeometryCollection` containing `Point`s (`coll_pt`, `coll_poly_pt`) match `WITH NOINDEX`
LET $stray_hole = {
	type: "Polygon",
	coordinates: [
		[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]],
		[[50.0, 0.0], [51.0, 0.0], [51.0, 1.0], [50.0, 1.0], [50.0, 0.0]]
	]
};
LET $stray_line = {
	type: "LineString",
	coordinates: [[0.5, 40.0], [50.5, 0.5]]
};
CREATE nf_geoms:stray_hole SET g = $stray_hole RETURN NONE;
SELECT VALUE id FROM nf_geoms WHERE g INTERSECTS $stray_line ORDER BY id;
SELECT VALUE id FROM nf_geoms WITH NOINDEX WHERE g INTERSECTS $stray_line ORDER BY id;

CREATE nf_geoms:stray_line SET g = $stray_line RETURN NONE;
SELECT VALUE id FROM nf_geoms WHERE g INTERSECTS $stray_hole ORDER BY id;
SELECT VALUE id FROM nf_geoms WITH NOINDEX WHERE g INTERSECTS $stray_hole ORDER BY id;

CREATE nf_geoms:coll_pt SET g = {
	type: "GeometryCollection",
	geometries: [{ type: "Point", coordinates: [2.35, 48.85] }]
} RETURN NONE;
CREATE nf_geoms:coll_poly_pt SET g = {
	type: "GeometryCollection",
	geometries: [
		{ type: "Polygon", coordinates: [[[2.0, 48.0], [3.0, 48.0], [3.0, 49.0], [2.0, 49.0], [2.0, 48.0]]] },
		{ type: "Point", coordinates: [2.35, 48.85] }
	]
} RETURN NONE;
SELECT VALUE id FROM nf_geoms WHERE g INTERSECTS (2.35, 48.85) ORDER BY id;
SELECT VALUE id FROM nf_geoms WITH NOINDEX WHERE g INTERSECTS (2.35, 48.85) ORDER BY id;
SELECT VALUE id FROM nf_geoms WHERE g CONTAINS (2.35, 48.85) ORDER BY id;
SELECT VALUE id FROM nf_geoms WITH NOINDEX WHERE g CONTAINS (2.35, 48.85) ORDER BY id;

-- 20. Multi-clause OR on a mixed-shape spatial table (`retro_shapes`, where `include_ancestors` is
--     true): an 8-branch OR crossing the prime meridian (`$uk_box`) keeps a selective index union
--     (`BitmapScan` over the `or_union_from_expr` branches), whereas a 49-branch OR (which receives
--     per-branch budget `max_ops = 1` and collapses to the root range when `include_ancestors` is
--     true) declines the degenerate full-index branch and falls back to `TableScan`.
LET $e_or8_mixed = (
	SELECT VALUE id FROM retro_shapes
	WHERE g INSIDE $uk_box OR g INSIDE $fr_box OR g CONTAINS (-0.1, 51.5) OR g CONTAINS (-0.2, 51.5)
	   OR g CONTAINS (-0.3, 51.5) OR g CONTAINS (-0.4, 51.5) OR g CONTAINS (0.1, 51.5) OR g CONTAINS (0.2, 51.5)
	EXPLAIN
);
LET $e_or49_mixed = (
	SELECT VALUE id FROM retro_shapes
	WHERE g INSIDE $uk_box
	   OR g CONTAINS (-0.01, 51.5) OR g CONTAINS (-0.02, 51.5) OR g CONTAINS (-0.03, 51.5) OR g CONTAINS (-0.04, 51.5)
	   OR g CONTAINS (-0.05, 51.5) OR g CONTAINS (-0.06, 51.5) OR g CONTAINS (-0.07, 51.5) OR g CONTAINS (-0.08, 51.5)
	   OR g CONTAINS (-0.09, 51.5) OR g CONTAINS (-0.10, 51.5) OR g CONTAINS (-0.11, 51.5) OR g CONTAINS (-0.12, 51.5)
	   OR g CONTAINS (-0.13, 51.5) OR g CONTAINS (-0.14, 51.5) OR g CONTAINS (-0.15, 51.5) OR g CONTAINS (-0.16, 51.5)
	   OR g CONTAINS (-0.17, 51.5) OR g CONTAINS (-0.18, 51.5) OR g CONTAINS (-0.19, 51.5) OR g CONTAINS (-0.20, 51.5)
	   OR g CONTAINS (-0.21, 51.5) OR g CONTAINS (-0.22, 51.5) OR g CONTAINS (-0.23, 51.5) OR g CONTAINS (-0.24, 51.5)
	   OR g CONTAINS (0.01, 51.5)  OR g CONTAINS (0.02, 51.5)  OR g CONTAINS (0.03, 51.5)  OR g CONTAINS (0.04, 51.5)
	   OR g CONTAINS (0.05, 51.5)  OR g CONTAINS (0.06, 51.5)  OR g CONTAINS (0.07, 51.5)  OR g CONTAINS (0.08, 51.5)
	   OR g CONTAINS (0.09, 51.5)  OR g CONTAINS (0.10, 51.5)  OR g CONTAINS (0.11, 51.5)  OR g CONTAINS (0.12, 51.5)
	   OR g CONTAINS (0.13, 51.5)  OR g CONTAINS (0.14, 51.5)  OR g CONTAINS (0.15, 51.5)  OR g CONTAINS (0.16, 51.5)
	   OR g CONTAINS (0.17, 51.5)  OR g CONTAINS (0.18, 51.5)  OR g CONTAINS (0.19, 51.5)  OR g CONTAINS (0.20, 51.5)
	   OR g CONTAINS (0.21, 51.5)  OR g CONTAINS (0.22, 51.5)  OR g CONTAINS (0.23, 51.5)  OR g CONTAINS (0.24, 51.5)
	EXPLAIN
);
RETURN [
	$e_or8_mixed.children[0].operator,
	$e_or8_mixed.children[0].children[0].operator,
	$e_or49_mixed.children[0].operator
];

-- 21. Nullish (`NONE` and `NULL`) values on a schemaless table record `ValueShape::NonGeometry`
--     so `INSIDE` keeps the point-only index scan while `geo::distance` declines index pushdown
--     and raises the same type error as `WITH NOINDEX`
DEFINE TABLE nullish_mixed;
DEFINE INDEX idx_nullish_mixed ON nullish_mixed FIELDS loc SPATIAL;
CREATE nullish_mixed:a_none SET loc = NONE RETURN NONE;
CREATE nullish_mixed:b_null SET loc = NULL RETURN NONE;
CREATE nullish_mixed:pt     SET loc = (-0.12, 51.51) RETURN NONE;

SELECT VALUE id FROM nullish_mixed WHERE loc INSIDE $uk_box ORDER BY id;
SELECT VALUE id FROM nullish_mixed WITH NOINDEX WHERE loc INSIDE $uk_box ORDER BY id;
SELECT VALUE id FROM nullish_mixed WHERE geo::distance(loc, (-0.12, 51.51)) < 50000.0 ORDER BY id;
SELECT VALUE id FROM nullish_mixed WITH NOINDEX WHERE geo::distance(loc, (-0.12, 51.51)) < 50000.0 ORDER BY id;

