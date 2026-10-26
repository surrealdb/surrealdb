/**
[bench]
reason = "Geo compound prefix + containment scan: equality prefix on category combined with INSIDE polygon (unindexed full scan vs compound SPATIAL index)"
run = true
rebuild = false
datasets = { unindexed = "bench/util/geo-points-100000.surql", indexed = "bench/util/geo-points-100000-indexed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM place WHERE category = 'cafe' AND location INSIDE {
	type: 'Polygon',
	coordinates: [[[-10.0, 35.0], [40.0, 35.0], [40.0, 70.0], [-10.0, 70.0], [-10.0, 35.0]]]
};
