/**
# Deep-traversal dataset: a complete ternary tree of depth 9 — 29524
# `person` vertices where node `i` links to children `3i-1`, `3i`, `3i+1`
# — in a classic (record-backed) relation. The existing recursion benches
# cap at 6 hops over small neighbourhoods; this tree makes a 9-hop bounded
# recursion collect the whole 29K-node frontier, so depth dominates. The
# lightweight twin (`graph-deep-lightweight`) holds the identical tree
# record-less, and `graph/deep_tree_folded` folds this one into numeric
# blocks. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS;
	DEFINE TABLE knows TYPE RELATION IN person OUT person;

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
