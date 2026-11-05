/**
# Lightweight twin of `graph-deep-classic`: the identical depth-9 ternary
# tree (29524 vertices, node `i` linking to `3i-1`, `3i`, `3i+1`) in a
# record-less LIGHTWEIGHT relation. Gives the `graph/deep_tree` bench its
# record-less side. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS;
	DEFINE TABLE knows TYPE RELATION IN person OUT person LIGHTWEIGHT;

	CREATE |person:1..=29524| RETURN NONE;

	FOR $i IN 1..=9841 {
		LET $s = type::record('person', $i);
		LET $a = type::record('person', $i * 3 - 1);
		LET $b = type::record('person', $i * 3);
		LET $c = type::record('person', $i * 3 + 1);
		RELATE $s->knows->$a;
		RELATE $s->knows->$b;
		RELATE $s->knows->$c;
	};

	RETURN 'OK';
}
