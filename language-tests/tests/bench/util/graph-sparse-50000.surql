/**
# Larger-scale sparse graph dataset: 50000 `person` nodes, each with ~2 random
# `knows` edges (same shape as `graph-sparse-5000`, 10x the node count). Used as
# the `large` arm of the two-hop / shortest-path benches to see how multi-hop
# traversal cost changes with graph *size* rather than degree — `graph-dense-5000`
# already covers the degree axis at the small scale. Built with RELATE so the
# graph adjacency keys exist. `run = false`.

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
		LET $a = type::record('person', rand::int(1, 50000));
		LET $b = type::record('person', rand::int(1, 50000));
		RELATE $p->knows->$a;
		RELATE $p->knows->$b;
	};

	RETURN 'OK';
}
