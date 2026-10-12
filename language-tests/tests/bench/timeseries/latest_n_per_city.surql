/**
[bench]
reason = "The ten latest readings of one city: ORDER BY time DESC LIMIT 10 after an equality on the first component of the record id. The keyed arm reads that city's key range backwards; a datetime component orders its keys by the whole second only, so the top-k stays and the scan stops once it passes a second beyond the tenth row it keeps. The unkeyed arm answers it from the compound (city, time) index. Both arms return the same ten rows, so their row counts must match."
run = true
rebuild = false
datasets = { keyed = "bench/util/readings-keyed-100000.surql", unkeyed = "bench/util/readings-unkeyed-100000-indexed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM reading WHERE city = 'city-7' ORDER BY time DESC LIMIT 10;
