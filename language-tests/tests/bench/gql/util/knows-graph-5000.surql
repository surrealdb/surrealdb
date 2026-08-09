/**
# GQL bench dataset: 5000 `person` nodes, each with 4 deterministic
# outgoing `knows` edges to the next four ids (wrapping modulo 5000). Built with
# RELATE so the graph adjacency keys exist — a bare INSERT into the edge table
# would skip them, and GQL `MATCH (a)-[:knows]->(b)` lowers to a graph
# expansion that reads those keys.
#
# Deterministic by construction (no rand): id n knows n+1, n+2, n+3, n+4 (mod
# 5000, in 1..=5000), giving exactly 20000 edges and a constant out-degree of 4.
# Re-importing always produces byte-identical adjacency, so the single-hop
# `expand` bench and the shared-variable `join` bench measure a stable workload.
# `run = false` — imported only by benches, never run as a correctness test.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS;

	CREATE |person:1..=5000| SET name = 'p' RETURN NONE;

	FOR $n IN array::range(1, 5001) {
		LET $s = type::record('person', $n);
		FOR $d IN [1, 2, 3, 4] {
			-- wrap into 1..=5000: ((n - 1 + d) mod 5000) + 1
			LET $t = type::record('person', (($n - 1 + $d) % 5000) + 1);
			RELATE $s->knows->$t;
		};
	};

	RETURN 'OK';
}
