/**
[env]

[test]
reason = "Differential spatial operator battery: every spatial predicate (INSIDE, CONTAINS, INTERSECTS, CONTAINSALL, CONTAINSANY, ALLINSIDE, ANYINSIDE, in both operand orders) across Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, and GeometryCollection is executed with the SPATIAL index and paired with its WITH NOINDEX ground-truth twin."

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
value = "NONE"

[[test.results]]
value = "[places:london, places:oxford]"

[[test.results]]
value = "[places:london, places:oxford]"

[[test.results]]
value = "[places:london, places:oxford]"

[[test.results]]
value = "[places:london, places:oxford]"

[[test.results]]
value = "[places:london, places:oxford, places:paris]"

[[test.results]]
value = "[places:london, places:oxford, places:paris]"

[[test.results]]
value = "[places:london, places:oxford, places:paris]"

[[test.results]]
value = "[places:london, places:oxford, places:paris]"

[[test.results]]
value = "[places:london, places:oxford]"

[[test.results]]
value = "[places:london, places:oxford]"

[[test.results]]
value = "[places:paris]"

[[test.results]]
value = "[places:paris]"

[[test.results]]
value = "[places:paris]"

[[test.results]]
value = "[places:paris]"

[[test.results]]
value = "[places:london]"

[[test.results]]
value = "[places:london]"

[[test.results]]
value = "[places:london, places:tokyo]"

[[test.results]]
value = "[places:london, places:tokyo]"

[[test.results]]
value = "[places:london, places:tokyo]"

[[test.results]]
value = "[places:london, places:tokyo]"

[[test.results]]
value = "[places:london]"

[[test.results]]
value = "[places:london]"

[[test.results]]
value = "[places:london]"

[[test.results]]
value = "[places:london]"

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
value = "[zones:capitals_mpt, zones:uk_box]"

[[test.results]]
value = "[zones:capitals_mpt, zones:uk_box]"

[[test.results]]
value = "[zones:capitals_mpt, zones:uk_box]"

[[test.results]]
value = "[zones:capitals_mpt, zones:uk_box]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[zones:fr_box]"

[[test.results]]
value = "[zones:fr_box]"

[[test.results]]
value = "[zones:fr_box]"

[[test.results]]
value = "[zones:fr_box]"

[[test.results]]
value = "[zones:fr_box]"

[[test.results]]
value = "[zones:fr_box]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[zones:benelux_mp, zones:channel_line, zones:fr_box, zones:uk_box]"

[[test.results]]
value = "[zones:benelux_mp, zones:channel_line, zones:fr_box, zones:uk_box]"

[[test.results]]
value = "[zones:benelux_mp, zones:channel_line, zones:fr_box, zones:uk_box]"

[[test.results]]
value = "[zones:benelux_mp, zones:channel_line, zones:fr_box, zones:uk_box]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[zones:channel_line, zones:fr_box, zones:uk_box]"

[[test.results]]
value = "[zones:channel_line, zones:fr_box, zones:uk_box]"

[[test.results]]
value = "[zones:channel_line, zones:fr_box, zones:uk_box]"

[[test.results]]
value = "[zones:channel_line, zones:fr_box, zones:uk_box]"

[[test.results]]
value = "[zones:fr_box]"

[[test.results]]
value = "[zones:fr_box]"

[[test.results]]
value = "[zones:fr_box]"

[[test.results]]
value = "[zones:fr_box]"

[[test.results]]
value = "[zones:capitals_mpt, zones:jp_col, zones:uk_box]"

[[test.results]]
value = "[zones:capitals_mpt, zones:jp_col, zones:uk_box]"

[[test.results]]
value = "[zones:capitals_mpt, zones:jp_col, zones:uk_box]"

[[test.results]]
value = "[zones:capitals_mpt, zones:jp_col, zones:uk_box]"

[[test.results]]
value = "[places:london]"

[[test.results]]
value = "[places:london]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[0]"

[[test.results]]
value = "[0]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Filter', 'UnionIndexScan', '17', 'DistinctRecords', 'idx_zones_geom']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['IndexCountScan', 'BitmapOr', 'BitmapIndexScan', 'idx_places_pt', 'pt INSIDE (-0.12, 51.51)']"
*/

-- 1. Point-only schemaful table (`point_only_spatial` optimization active)
DEFINE TABLE places SCHEMAFULL;
DEFINE FIELD pt ON places TYPE geometry<point>;
DEFINE INDEX idx_places_pt ON places FIELDS pt SPATIAL;

CREATE places:london SET pt = (-0.12, 51.51) RETURN NONE;
CREATE places:oxford SET pt = (-1.25, 51.75) RETURN NONE;
CREATE places:paris  SET pt = (2.35, 48.85) RETURN NONE;
CREATE places:berlin SET pt = (13.40, 52.52) RETURN NONE;
CREATE places:tokyo  SET pt = (139.69, 35.68) RETURN NONE;
CREATE places:sydney SET pt = (151.21, -33.87) RETURN NONE;

LET $uk_poly = {
	type: "Polygon",
	coordinates: [[[-2.0, 51.0], [0.5, 51.0], [0.5, 52.5], [-2.0, 52.5], [-2.0, 51.0]]]
};
LET $fr_poly = {
	type: "Polygon",
	coordinates: [[[1.5, 48.0], [3.5, 48.0], [3.5, 49.5], [1.5, 49.5], [1.5, 48.0]]]
};
LET $jp_poly = {
	type: "Polygon",
	coordinates: [[[138.5, 34.5], [140.5, 34.5], [140.5, 36.5], [138.5, 36.5], [138.5, 34.5]]]
};
LET $eur_poly = {
	type: "Polygon",
	coordinates: [[[-5.0, 45.0], [15.0, 45.0], [15.0, 55.0], [-5.0, 55.0], [-5.0, 45.0]]]
};
LET $uk_fr_multipoly = {
	type: "MultiPolygon",
	coordinates: [
		[[[-2.0, 51.0], [0.5, 51.0], [0.5, 52.5], [-2.0, 52.5], [-2.0, 51.0]]],
		[[[1.5, 48.0], [3.5, 48.0], [3.5, 49.5], [1.5, 49.5], [1.5, 48.0]]]
	]
};
LET $eur_uk_collection = {
	type: "GeometryCollection",
	geometries: [
		{
			type: "Polygon",
			coordinates: [[[-5.0, 45.0], [15.0, 45.0], [15.0, 55.0], [-5.0, 55.0], [-5.0, 45.0]]]
		},
		{
			type: "Polygon",
			coordinates: [[[-2.0, 51.0], [0.5, 51.0], [0.5, 52.5], [-2.0, 52.5], [-2.0, 51.0]]]
		}
	]
};

-- INSIDE Polygon (direct & reversed CONTAINS)
SELECT VALUE id FROM places WHERE pt INSIDE $uk_poly ORDER BY id;
SELECT VALUE id FROM places WITH NOINDEX WHERE pt INSIDE $uk_poly ORDER BY id;

SELECT VALUE id FROM places WHERE $uk_poly CONTAINS pt ORDER BY id;
SELECT VALUE id FROM places WITH NOINDEX WHERE $uk_poly CONTAINS pt ORDER BY id;

-- INSIDE MultiPolygon (direct & reversed CONTAINS)
SELECT VALUE id FROM places WHERE pt INSIDE $uk_fr_multipoly ORDER BY id;
SELECT VALUE id FROM places WITH NOINDEX WHERE pt INSIDE $uk_fr_multipoly ORDER BY id;

SELECT VALUE id FROM places WHERE $uk_fr_multipoly CONTAINS pt ORDER BY id;
SELECT VALUE id FROM places WITH NOINDEX WHERE $uk_fr_multipoly CONTAINS pt ORDER BY id;

-- INSIDE GeometryCollection
SELECT VALUE id FROM places WHERE pt INSIDE $eur_uk_collection ORDER BY id;
SELECT VALUE id FROM places WITH NOINDEX WHERE pt INSIDE $eur_uk_collection ORDER BY id;

-- INTERSECTS Polygon & Point (both operand orders)
SELECT VALUE id FROM places WHERE pt INTERSECTS $fr_poly ORDER BY id;
SELECT VALUE id FROM places WITH NOINDEX WHERE pt INTERSECTS $fr_poly ORDER BY id;

SELECT VALUE id FROM places WHERE $fr_poly INTERSECTS pt ORDER BY id;
SELECT VALUE id FROM places WITH NOINDEX WHERE $fr_poly INTERSECTS pt ORDER BY id;

SELECT VALUE id FROM places WHERE pt INTERSECTS (-0.12, 51.51) ORDER BY id;
SELECT VALUE id FROM places WITH NOINDEX WHERE pt INTERSECTS (-0.12, 51.51) ORDER BY id;

-- CONTAINSANY / ANYINSIDE with array of candidate points
SELECT VALUE id FROM places WHERE pt CONTAINSANY [(-0.12, 51.51), (139.69, 35.68)] ORDER BY id;
SELECT VALUE id FROM places WITH NOINDEX WHERE pt CONTAINSANY [(-0.12, 51.51), (139.69, 35.68)] ORDER BY id;

SELECT VALUE id FROM places WHERE [(-0.12, 51.51), (139.69, 35.68)] ANYINSIDE pt ORDER BY id;
SELECT VALUE id FROM places WITH NOINDEX WHERE [(-0.12, 51.51), (139.69, 35.68)] ANYINSIDE pt ORDER BY id;

-- CONTAINSALL / ALLINSIDE with single-point array
SELECT VALUE id FROM places WHERE pt CONTAINSALL [(-0.12, 51.51)] ORDER BY id;
SELECT VALUE id FROM places WITH NOINDEX WHERE pt CONTAINSALL [(-0.12, 51.51)] ORDER BY id;

SELECT VALUE id FROM places WHERE [(-0.12, 51.51)] ALLINSIDE pt ORDER BY id;
SELECT VALUE id FROM places WITH NOINDEX WHERE [(-0.12, 51.51)] ALLINSIDE pt ORDER BY id;

-- 2. Mixed non-point geometries table (ancestor cell probes active)
DEFINE TABLE zones;
DEFINE INDEX idx_zones_geom ON zones FIELDS geom SPATIAL;

CREATE zones:uk_box SET geom = {
	type: "Polygon",
	coordinates: [[[-1.0, 51.0], [0.5, 51.0], [0.5, 52.0], [-1.0, 52.0], [-1.0, 51.0]]]
} RETURN NONE;
CREATE zones:fr_box SET geom = {
	type: "Polygon",
	coordinates: [[[2.0, 48.5], [3.0, 48.5], [3.0, 49.2], [2.0, 49.2], [2.0, 48.5]]]
} RETURN NONE;
CREATE zones:channel_line SET geom = {
	type: "LineString",
	coordinates: [[-0.12, 51.51], [2.35, 48.85]]
} RETURN NONE;
CREATE zones:benelux_mp SET geom = {
	type: "MultiPolygon",
	coordinates: [
		[[[4.0, 50.5], [5.0, 50.5], [5.0, 51.5], [4.0, 51.5], [4.0, 50.5]]],
		[[[4.5, 52.0], [5.5, 52.0], [5.5, 52.8], [4.5, 52.8], [4.5, 52.0]]]
	]
} RETURN NONE;
CREATE zones:capitals_mpt SET geom = {
	type: "MultiPoint",
	coordinates: [[-0.12, 51.51], [2.35, 48.85]]
} RETURN NONE;
CREATE zones:jp_col SET geom = {
	type: "GeometryCollection",
	geometries: [
		{ type: "Point", coordinates: [139.69, 35.68] },
		{
			type: "Polygon",
			coordinates: [[[139.0, 35.0], [140.0, 35.0], [140.0, 36.0], [139.0, 36.0], [139.0, 35.0]]]
		}
	]
} RETURN NONE;

-- Stored geometry CONTAINS Point (direct & reversed INSIDE)
SELECT VALUE id FROM zones WHERE geom CONTAINS (-0.12, 51.51) ORDER BY id;
SELECT VALUE id FROM zones WITH NOINDEX WHERE geom CONTAINS (-0.12, 51.51) ORDER BY id;

SELECT VALUE id FROM zones WHERE (-0.12, 51.51) INSIDE geom ORDER BY id;
SELECT VALUE id FROM zones WITH NOINDEX WHERE (-0.12, 51.51) INSIDE geom ORDER BY id;

-- Stored geometry CONTAINS LineString / Polygon / MultiPolygon
LET $paris_line = {
	type: "LineString",
	coordinates: [[2.2, 48.7], [2.5, 48.9]]
};
LET $paris_subpoly = {
	type: "Polygon",
	coordinates: [[[2.2, 48.7], [2.5, 48.7], [2.5, 48.9], [2.2, 48.9], [2.2, 48.7]]]
};
LET $paris_multipoly = {
	type: "MultiPolygon",
	coordinates: [[[[2.2, 48.7], [2.5, 48.7], [2.5, 48.9], [2.2, 48.9], [2.2, 48.7]]]]
};
LET $tokyo_multipoly = {
	type: "MultiPolygon",
	coordinates: [[[[139.2, 35.2], [139.8, 35.2], [139.8, 35.8], [139.2, 35.8], [139.2, 35.2]]]]
};

SELECT VALUE id FROM zones WHERE geom CONTAINS $paris_line ORDER BY id;
SELECT VALUE id FROM zones WITH NOINDEX WHERE geom CONTAINS $paris_line ORDER BY id;

SELECT VALUE id FROM zones WHERE geom CONTAINS $paris_subpoly ORDER BY id;
SELECT VALUE id FROM zones WITH NOINDEX WHERE geom CONTAINS $paris_subpoly ORDER BY id;

SELECT VALUE id FROM zones WHERE geom CONTAINS $paris_multipoly ORDER BY id;
SELECT VALUE id FROM zones WITH NOINDEX WHERE geom CONTAINS $paris_multipoly ORDER BY id;

SELECT VALUE id FROM zones WHERE geom CONTAINS $tokyo_multipoly ORDER BY id;
SELECT VALUE id FROM zones WITH NOINDEX WHERE geom CONTAINS $tokyo_multipoly ORDER BY id;

-- Stored geometry INSIDE larger Polygon (direct & reversed CONTAINS)
SELECT VALUE id FROM zones WHERE geom INSIDE $eur_poly ORDER BY id;
SELECT VALUE id FROM zones WITH NOINDEX WHERE geom INSIDE $eur_poly ORDER BY id;

SELECT VALUE id FROM zones WHERE $eur_poly CONTAINS geom ORDER BY id;
SELECT VALUE id FROM zones WITH NOINDEX WHERE $eur_poly CONTAINS geom ORDER BY id;

-- Stored geometry INTERSECTS LineString / Polygon (direct & reversed)
LET $cross_channel = {
	type: "LineString",
	coordinates: [[-0.5, 51.5], [2.5, 48.8]]
};
SELECT VALUE id FROM zones WHERE geom INTERSECTS $cross_channel ORDER BY id;
SELECT VALUE id FROM zones WITH NOINDEX WHERE geom INTERSECTS $cross_channel ORDER BY id;

SELECT VALUE id FROM zones WHERE $cross_channel INTERSECTS geom ORDER BY id;
SELECT VALUE id FROM zones WITH NOINDEX WHERE $cross_channel INTERSECTS geom ORDER BY id;

-- Stored geometry CONTAINSALL / ALLINSIDE array of points
SELECT VALUE id FROM zones WHERE geom CONTAINSALL [(2.2, 48.7), (2.5, 48.9)] ORDER BY id;
SELECT VALUE id FROM zones WITH NOINDEX WHERE geom CONTAINSALL [(2.2, 48.7), (2.5, 48.9)] ORDER BY id;

SELECT VALUE id FROM zones WHERE [(2.2, 48.7), (2.5, 48.9)] ALLINSIDE geom ORDER BY id;
SELECT VALUE id FROM zones WITH NOINDEX WHERE [(2.2, 48.7), (2.5, 48.9)] ALLINSIDE geom ORDER BY id;

-- Stored geometry CONTAINSANY / ANYINSIDE array of points
SELECT VALUE id FROM zones WHERE geom CONTAINSANY [(-0.12, 51.51), (139.69, 35.68)] ORDER BY id;
SELECT VALUE id FROM zones WITH NOINDEX WHERE geom CONTAINSANY [(-0.12, 51.51), (139.69, 35.68)] ORDER BY id;

SELECT VALUE id FROM zones WHERE [(-0.12, 51.51), (139.69, 35.68)] ANYINSIDE geom ORDER BY id;
SELECT VALUE id FROM zones WITH NOINDEX WHERE [(-0.12, 51.51), (139.69, 35.68)] ANYINSIDE geom ORDER BY id;

-- Array literal in INSIDE (disjunctive element equality vs GeometryCollection) and exact bitmap COUNT
SELECT VALUE id FROM places WHERE pt INSIDE [$fr_poly, (-0.12, 51.51)] ORDER BY id;
SELECT VALUE id FROM places WITH NOINDEX WHERE pt INSIDE [$fr_poly, (-0.12, 51.51)] ORDER BY id;

SELECT VALUE count() FROM places WHERE pt INSIDE $uk_poly GROUP ALL;
SELECT VALUE count() FROM places WITH NOINDEX WHERE pt INSIDE $uk_poly GROUP ALL;

SELECT VALUE count() FROM places WHERE pt INSIDE [$uk_poly, $uk_poly] GROUP ALL;
SELECT VALUE count() FROM places WITH NOINDEX WHERE pt INSIDE [$uk_poly, $uk_poly] GROUP ALL;

-- EXPLAIN plan verification for non-point-only index (probing ancestor cells + Filter)
-- and zero-fetch bitmap COUNT
LET $e_zones = (SELECT VALUE id FROM zones WHERE geom CONTAINS (-0.12, 51.51) EXPLAIN);
RETURN [
	$e_zones.children[0].operator,
	$e_zones.children[0].children[0].operator,
	$e_zones.children[0].children[0].attributes.branches,
	$e_zones.children[0].children[0].children[0].operator,
	$e_zones.children[0].children[0].children[0].children[0].attributes.index
];
LET $e_cnt = (SELECT VALUE count() FROM places WHERE pt INSIDE (-0.12, 51.51) GROUP ALL EXPLAIN);
RETURN [
	$e_cnt.children[0].operator,
	$e_cnt.children[0].children[0].operator,
	$e_cnt.children[0].children[0].children[0].operator,
	$e_cnt.children[0].children[0].children[0].attributes.index,
	$e_cnt.children[0].children[0].children[0].attributes.point_filter
];


