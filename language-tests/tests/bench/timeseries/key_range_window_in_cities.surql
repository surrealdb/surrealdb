/**
[bench]
reason = "The same one-hour window over three cities at once, about 250 rows: an IN list on the first component of the record id and a range on the second. The keyed arm reads one key range per city and keeps the predicate as a filter; the unkeyed arm answers it from the compound (city, time) index. Both arms return the same rows, so their row counts must match."
run = true
rebuild = false
datasets = { keyed = "bench/util/readings-keyed-100000.surql", unkeyed = "bench/util/readings-unkeyed-100000-indexed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT city, time, temp FROM reading WHERE city IN ['city-7', 'city-21', 'city-42'] AND time >= d'2024-01-01T06:00:00.500Z' AND time < d'2024-01-01T07:00:00.500Z';
