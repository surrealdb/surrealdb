/**
[bench]
reason = "Geo narrow proximity projected scan: `SELECT id, category` with `geo::distance` using selective field extraction (`DecodeFields::Projected`) after inline `SpatialPointFilter` (unindexed full scan vs SPATIAL index)"
run = true
rebuild = false
datasets = { unindexed = "bench/util/geo-points-100000.surql", indexed = "bench/util/geo-points-100000-indexed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, category FROM place WHERE geo::distance(location, (-0.1276, 51.5072)) < 200000;
