/**
[bench]
reason = "Geo containment scan: point-in-polygon (INSIDE) against a fixed bounding polygon (unindexed full scan vs SPATIAL index)"
run = true
rebuild = false
datasets = { unindexed = "bench/util/geo-points-100000.surql", indexed = "bench/util/geo-points-100000-indexed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM place WHERE location INSIDE {
	type: 'Polygon',
	coordinates: [[[-10.0, 35.0], [40.0, 35.0], [40.0, 70.0], [-10.0, 70.0], [-10.0, 35.0]]]
};
