/**
# Hub-skewed graph dataset: 50000 `person` nodes where 20 designated hubs
# (`person:1`..`person:20`) each have 250 outgoing `knows` edges and the
# remaining 49980 nodes have 2, mimicking the degree skew of a real social
# graph. `graph-sparse-5000` / `graph-dense-5000` vary degree *uniformly*
# across the whole graph, which never stresses a single-node, high-fan-out
# traversal — that's what the `graph/hub_fanout` bench (querying `person:1`
# directly) needs. Hub ids are fixed (not random) so the bench target is
# reproducible across runs. Built with RELATE so the graph adjacency keys
# exist. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS;

	CREATE |person:1..=50000| SET
		name = rand::string(12),
		age = rand::int(18, 80)
	RETURN NONE;

	FOR $p IN (SELECT VALUE id FROM person) {
		LET $degree = IF record::id($p) <= 20 { 250 } ELSE { 2 };
		FOR $_ IN array::range(0, $degree) {
			LET $t = type::record('person', rand::int(1, 50000));
			RELATE $p->knows->$t;
		};
	};

	RETURN 'OK';
}
