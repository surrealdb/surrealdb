/**
# Plain twin of `graph-filtered-100k-inline`: the identical 100000-edge
# hub graph with the identical deterministic scores, but without the
# INLINE clause on `score` — every predicate evaluation must fetch the
# edge record. Gives the `graph/filtered_100k` bench its baseline side.
# `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS;
	DEFINE TABLE knows TYPE RELATION;
	DEFINE FIELD score ON knows TYPE number;

	CREATE |person:1..=100001| RETURN NONE;

	FOR $n IN 1..=100000 {
		LET $t = type::record('person', $n + 1);
		RELATE person:1->knows->$t SET score = $n % 10000;
	};

	RETURN 'OK';
}
