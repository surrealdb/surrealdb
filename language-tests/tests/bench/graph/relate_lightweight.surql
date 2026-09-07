/**
[bench]
reason = "RELATE throughput into a lightweight relation: each edge is two blind key writes and no record, against the classic relation's four keys plus a record write in `graph/relate_classic`. The pair also measures the storage-footprint claim — the datasets differ only in the LIGHTWEIGHT flag."
run = true
rebuild = true
datasets = { lightweight = "bench/util/graph-relate-lightweight.surql", classic = "bench/util/graph-relate-classic.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

FOR $n IN array::range(1, 1000) {
	LET $s = type::record('person', rand::int(1, 1000));
	LET $t = type::record('person', rand::int(1, 1000));
	RELATE $s->knows->$t;
};
