/**
[bench]
reason = "One city's readings over a one-hour window, about 83 of its 2000 rows: an equality on the first component of the record id and a range on the second. The keyed arm reads the window as a range of the record keys, with the predicate kept as a filter above the scan because a datetime component orders by the whole second; the unkeyed arm stores city and time in the body under random ids and answers it from the compound (city, time) index. The bounds lie inside a second, so each side of the keyed range is widened to the whole second and the filter drops the rows at the edges. Both arms return the same rows, so their row counts must match."
run = true
rebuild = false
datasets = { keyed = "bench/util/readings-keyed-100000.surql", unkeyed = "bench/util/readings-unkeyed-100000-indexed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT city, time, temp FROM reading WHERE city = 'city-7' AND time >= d'2024-01-01T06:00:00.500Z' AND time < d'2024-01-01T07:00:00.500Z';
