/**
# GQL bench dataset: a directed ring lattice of 2000 `node` rows. Each node
# n has out-degree 2 along `link` edges, to n+1 and n+2 (mod 2000, in 1..=2000).
# Built with RELATE so the graph adjacency keys exist.
#
# The bounded out-degree keeps variable-length expansion tractable: a `{1,4}`
# quantified MATCH emits one path per (start, depth, distinct-edge-walk), i.e.
# 2 + 4 + 8 + 16 = 30 paths per start node -> 60000 paths total. Big enough to
# stress PathExpand's per-path bookkeeping, bounded enough to terminate fast.
# Deterministic by construction (no rand), so re-imports are byte-identical.
# `run = false` — imported only by benches, never run as a correctness test.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE node SCHEMALESS;

	CREATE |node:1..=2000| SET name = 'n' RETURN NONE;

	FOR $n IN array::range(1, 2001) {
		LET $s = type::record('node', $n);
		FOR $d IN [1, 2] {
			-- wrap into 1..=2000: ((n - 1 + d) mod 2000) + 1
			LET $t = type::record('node', (($n - 1 + $d) % 2000) + 1);
			RELATE $s->link->$t;
		};
	};

	RETURN 'OK';
}
