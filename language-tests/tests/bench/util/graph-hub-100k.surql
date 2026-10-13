/**
# Single-vertex extreme-degree dataset: one hub `person` with 100000
# outgoing `knows` edges to 100000 distinct targets. `graph-hub-50000`
# spreads 250-degree hubs across a skewed graph; this dataset instead
# concentrates everything on one vertex, which is where per-key adjacency
# overhead dominates and packed blocks pay off — the `graph/hub_100k_*`
# bench family traverses `person:1` over this graph plain, folded, and
# folded with numeric block entries. Deterministic targets so every
# variant reads an identical graph. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS;
	DEFINE TABLE knows TYPE RELATION;

	CREATE |person:1..=100001| RETURN NONE;

	FOR $n IN 1..=100000 {
		LET $t = type::record('person', $n + 1);
		RELATE person:1->knows->$t;
	};

	RETURN 'OK';
}
