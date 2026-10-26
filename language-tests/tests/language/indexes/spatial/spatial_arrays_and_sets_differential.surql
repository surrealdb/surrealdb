/**
[env]

[test]
reason = "Differential spatial array and set battery: schemaful `array<geometry<point>>` (without `[*]`), schemaless `pts[*]` array, and `set<geometry<point>>` with `pts[*]`, tested across CONTAINS, INSIDE, CONTAINSALL, ALLINSIDE, CONTAINSANY, ANYINSIDE, empty-array vacuous truth, and duplicate-cell record deduplication — each paired with its WITH NOINDEX ground-truth twin."

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
value = "[routes:eurostar, routes:uk_local]"

[[test.results]]
value = "[routes:eurostar, routes:uk_local]"

[[test.results]]
value = "[routes:eurostar]"

[[test.results]]
value = "[routes:eurostar]"

[[test.results]]
value = "[routes:eurostar]"

[[test.results]]
value = "[routes:eurostar]"

[[test.results]]
value = "[routes:eurostar]"

[[test.results]]
value = "[routes:eurostar]"

[[test.results]]
value = "[routes:eurostar, routes:shinkansen]"

[[test.results]]
value = "[routes:eurostar, routes:shinkansen]"

[[test.results]]
value = "[routes:eurostar, routes:shinkansen]"

[[test.results]]
value = "[routes:eurostar, routes:shinkansen]"

[[test.results]]
value = "[routes:empty, routes:eurostar]"

[[test.results]]
value = "[routes:empty, routes:eurostar]"

[[test.results]]
value = "[routes:empty, routes:eurostar]"

[[test.results]]
value = "[routes:empty, routes:eurostar]"

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
value = "[trips:t1]"

[[test.results]]
value = "[trips:t1]"

[[test.results]]
value = "[trips:t1, trips:t2]"

[[test.results]]
value = "[trips:t1, trips:t2]"

[[test.results]]
value = "[trips:t1, trips:t3]"

[[test.results]]
value = "[trips:t1, trips:t3]"

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
value = "[hubs:eu]"

[[test.results]]
value = "[hubs:eu]"

[[test.results]]
value = "[hubs:jp]"

[[test.results]]
value = "[hubs:jp]"

[[test.results]]
value = "[hubs:eu]"

[[test.results]]
value = "[hubs:eu]"

[[test.results]]
value = "[hubs:eu, hubs:jp]"

[[test.results]]
value = "[hubs:eu, hubs:jp]"

[[test.results]]
value = "[hubs:eu]"

[[test.results]]
value = "[hubs:eu]"

[[test.results]]
value = "[hubs:eu, hubs:jp]"

[[test.results]]
value = "[hubs:eu, hubs:jp]"

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
value = "[]"

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
value = "[]"

[[test.results]]
value = "[flat_trips:f1, flat_trips:f2]"

[[test.results]]
value = "[flat_trips:f1, flat_trips:f2]"

[[test.results]]
value = "[flat_trips:f2, flat_trips:f3]"

[[test.results]]
value = "[flat_trips:f2, flat_trips:f3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['Filter', 'UnionIndexScan', '17', 'DistinctRecords', 'idx_routes_pts']"
*/

-- 1. Schemaful `array<geometry<point>>` indexed via `FIELDS pts SPATIAL` (without `[*]`)
DEFINE TABLE routes SCHEMAFULL;
DEFINE FIELD pts ON routes TYPE array<geometry<point>>;
DEFINE INDEX idx_routes_pts ON routes FIELDS pts SPATIAL;

LET $london   = (-0.12, 51.51);
LET $paris    = (2.35, 48.85);
LET $brussels = (4.35, 50.85);
LET $tokyo    = (139.69, 35.68);

CREATE routes:eurostar SET pts = [$london, $paris, $brussels] RETURN NONE;
CREATE routes:uk_local SET pts = [$london, (-0.120001, 51.510001)] RETURN NONE;
CREATE routes:shinkansen SET pts = [$tokyo, (135.50, 34.69)] RETURN NONE;
CREATE routes:empty SET pts = [] RETURN NONE;

-- CONTAINS / reversed INSIDE
SELECT VALUE id FROM routes WHERE pts CONTAINS $london ORDER BY id;
SELECT VALUE id FROM routes WITH NOINDEX WHERE pts CONTAINS $london ORDER BY id;

SELECT VALUE id FROM routes WHERE $paris INSIDE pts ORDER BY id;
SELECT VALUE id FROM routes WITH NOINDEX WHERE $paris INSIDE pts ORDER BY id;

-- CONTAINSALL / reversed ALLINSIDE
SELECT VALUE id FROM routes WHERE pts CONTAINSALL [$london, $paris] ORDER BY id;
SELECT VALUE id FROM routes WITH NOINDEX WHERE pts CONTAINSALL [$london, $paris] ORDER BY id;

SELECT VALUE id FROM routes WHERE [$london, $paris] ALLINSIDE pts ORDER BY id;
SELECT VALUE id FROM routes WITH NOINDEX WHERE [$london, $paris] ALLINSIDE pts ORDER BY id;

-- CONTAINSANY / reversed ANYINSIDE
SELECT VALUE id FROM routes WHERE pts CONTAINSANY [$paris, $tokyo] ORDER BY id;
SELECT VALUE id FROM routes WITH NOINDEX WHERE pts CONTAINSANY [$paris, $tokyo] ORDER BY id;

SELECT VALUE id FROM routes WHERE [$paris, $tokyo] ANYINSIDE pts ORDER BY id;
SELECT VALUE id FROM routes WITH NOINDEX WHERE [$paris, $tokyo] ANYINSIDE pts ORDER BY id;

-- Vacuous truth on empty array (`routes:empty` has `pts = []`):
-- `pts ALLINSIDE [...]` and `[...] CONTAINSALL pts` must include `routes:empty`
SELECT VALUE id FROM routes WHERE pts ALLINSIDE [$london, $paris, $brussels] ORDER BY id;
SELECT VALUE id FROM routes WITH NOINDEX WHERE pts ALLINSIDE [$london, $paris, $brussels] ORDER BY id;

SELECT VALUE id FROM routes WHERE [$london, $paris, $brussels] CONTAINSALL pts ORDER BY id;
SELECT VALUE id FROM routes WITH NOINDEX WHERE [$london, $paris, $brussels] CONTAINSALL pts ORDER BY id;

-- 2. Schemaless table with `FIELDS pts[*] SPATIAL`
DEFINE TABLE trips;
DEFINE INDEX idx_trips_pts ON trips FIELDS pts[*] SPATIAL;

CREATE trips:t1 SET pts = [$london, $paris] RETURN NONE;
CREATE trips:t2 SET pts = [$tokyo] RETURN NONE;
CREATE trips:t3 SET pts = [] RETURN NONE;

SELECT VALUE id FROM trips WHERE pts CONTAINS $paris ORDER BY id;
SELECT VALUE id FROM trips WITH NOINDEX WHERE pts CONTAINS $paris ORDER BY id;

SELECT VALUE id FROM trips WHERE pts CONTAINSANY [$london, $tokyo] ORDER BY id;
SELECT VALUE id FROM trips WITH NOINDEX WHERE pts CONTAINSANY [$london, $tokyo] ORDER BY id;

SELECT VALUE id FROM trips WHERE pts ALLINSIDE [$london, $paris] ORDER BY id;
SELECT VALUE id FROM trips WITH NOINDEX WHERE pts ALLINSIDE [$london, $paris] ORDER BY id;

-- 3. Schemaful `set<geometry<point>>` with `FIELDS pts[*] SPATIAL`
DEFINE TABLE hubs SCHEMAFULL;
DEFINE FIELD pts ON hubs TYPE set<geometry<point>>;
DEFINE INDEX idx_hubs_pts ON hubs FIELDS pts[*] SPATIAL;

CREATE hubs:eu SET pts = [$london, $paris, $brussels] RETURN NONE;
CREATE hubs:jp SET pts = [$tokyo] RETURN NONE;

SELECT VALUE id FROM hubs WHERE pts CONTAINS $london ORDER BY id;
SELECT VALUE id FROM hubs WITH NOINDEX WHERE pts CONTAINS $london ORDER BY id;

SELECT VALUE id FROM hubs WHERE $tokyo INSIDE pts ORDER BY id;
SELECT VALUE id FROM hubs WITH NOINDEX WHERE $tokyo INSIDE pts ORDER BY id;

SELECT VALUE id FROM hubs WHERE pts CONTAINSALL [$london, $brussels] ORDER BY id;
SELECT VALUE id FROM hubs WITH NOINDEX WHERE pts CONTAINSALL [$london, $brussels] ORDER BY id;

SELECT VALUE id FROM hubs WHERE pts CONTAINSANY [$paris, $tokyo] ORDER BY id;
SELECT VALUE id FROM hubs WITH NOINDEX WHERE pts CONTAINSANY [$paris, $tokyo] ORDER BY id;

-- Set literal operands (`{...}`) with CONTAINSALL and CONTAINSANY
SELECT VALUE id FROM hubs WHERE pts CONTAINSALL {(-0.12, 51.51), (4.35, 50.85)} ORDER BY id;
SELECT VALUE id FROM hubs WITH NOINDEX WHERE pts CONTAINSALL {(-0.12, 51.51), (4.35, 50.85)} ORDER BY id;

SELECT VALUE id FROM hubs WHERE pts CONTAINSANY {(2.35, 48.85), (139.69, 35.68)} ORDER BY id;
SELECT VALUE id FROM hubs WITH NOINDEX WHERE pts CONTAINSANY {(2.35, 48.85), (139.69, 35.68)} ORDER BY id;

-- 4. Bare `FIELDS loc SPATIAL` index with a single-element `Set` or GeoJSON Point in `Set`:
--    `spatial_column_shape` records `MultiCell` so residual filter elimination is not applied
DEFINE TABLE set_places;
DEFINE INDEX idx_set_places_loc ON set_places FIELDS loc SPATIAL;
CREATE set_places:a SET loc = <set>[(-0.12, 51.51)] RETURN NONE;
CREATE set_places:b SET loc = <set>[{ type: "Point", coordinates: [-0.12, 51.51] }] RETURN NONE;
LET $uk_sq = {
	type: "Polygon",
	coordinates: [[[-2.0, 51.0], [1.0, 51.0], [1.0, 52.5], [-2.0, 52.5], [-2.0, 51.0]]]
};
SELECT VALUE id FROM set_places WHERE loc INSIDE $uk_sq ORDER BY id;
SELECT VALUE id FROM set_places WITH NOINDEX WHERE loc INSIDE $uk_sq ORDER BY id;

-- 5. Flattened column `FIELDS pts... SPATIAL` indexes geometry elements of flattened arrays
DEFINE TABLE flat_trips;
DEFINE INDEX idx_flat_trips_pts ON flat_trips FIELDS pts... SPATIAL;
CREATE flat_trips:f1 SET pts = [$london] RETURN NONE;
CREATE flat_trips:f2 SET pts = [[$london, $paris], [$brussels]] RETURN NONE;
CREATE flat_trips:f3 SET pts = [[$tokyo]] RETURN NONE;

SELECT VALUE id FROM flat_trips WHERE pts... CONTAINS $london ORDER BY id;
SELECT VALUE id FROM flat_trips WITH NOINDEX WHERE pts... CONTAINS $london ORDER BY id;

SELECT VALUE id FROM flat_trips WHERE pts... CONTAINSANY [$paris, $tokyo] ORDER BY id;
SELECT VALUE id FROM flat_trips WITH NOINDEX WHERE pts... CONTAINSANY [$paris, $tokyo] ORDER BY id;

-- EXPLAIN plan verification for array<geometry<point>> spatial index
LET $e = (SELECT VALUE id FROM routes WHERE pts CONTAINS (-0.12, 51.51) EXPLAIN);
RETURN [
	$e.children[0].operator,
	$e.children[0].children[0].operator,
	$e.children[0].children[0].attributes.branches,
	$e.children[0].children[0].children[0].operator,
	$e.children[0].children[0].children[0].children[0].attributes.index
];


