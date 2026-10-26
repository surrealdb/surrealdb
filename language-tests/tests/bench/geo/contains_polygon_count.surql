/**
[bench]
reason = "Geo narrow containment count scan: `SELECT count() ... GROUP ALL` resolved via exact bitmap `IndexCountScan` on point-only SPATIAL index (unindexed full scan vs SPATIAL index)"
run = true
rebuild = false
datasets = { unindexed = "bench/util/geo-points-100000.surql", indexed = "bench/util/geo-points-100000-indexed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count() FROM place WHERE location INSIDE {
	type: 'Polygon',
	coordinates: [[[-2.5, 49.0], [2.5, 49.0], [2.5, 54.0], [-2.5, 54.0], [-2.5, 49.0]]]
} GROUP ALL;
