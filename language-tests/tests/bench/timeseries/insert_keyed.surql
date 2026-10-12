/**
[bench]
reason = "Create 1000 readings into an empty table, one CREATE per FOR iteration, each setting city and time. Keyed: the record id is built from the two components and nothing else is written for the key. Unkeyed: a random id, with city and time stored in the body. Unkeyed_indexed: the same, with the compound (city, time) index an unkeyed table needs to find readings by city and time, which the key of the keyed table makes unnecessary. All three write the same 1000 (city, time) pairs."
run = true
rebuild = true
datasets = { keyed = "bench/util/readings-keyed-empty.surql", unkeyed = "bench/util/readings-unkeyed-empty.surql", unkeyed_indexed = "bench/util/readings-unkeyed-indexed-empty.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

FOR $i IN 0..1000 {
	CREATE reading SET
		city = string::concat('city-', $i % 50),
		time = time::from_millis(1704067200000 + $i * 863),
		temp = (($i * 37) % 500) / 10.0 - 10.0,
		humidity = 20 + ($i * 13) % 80
		RETURN NONE;
};
