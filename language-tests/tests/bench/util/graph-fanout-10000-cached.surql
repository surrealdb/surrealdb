/**
# Wide fan-out dataset for the inline adjacency cache at scale: 10000
# `person` nodes, every one of them a source with 8 outgoing `knows` edges
# to deterministic targets. The vertex table carries `INLINE EDGES 16`, so
# the RELATEs materialise every source's cache — the `graph/fanout_10k`
# bench then measures a 10000-source fan-out served entirely from cache
# hits, one hundred times the source count of `graph/fanout_inline_cache`.
# `graph-fanout-10000-uncached` is the byte-identical twin without the
# cap. Deterministic targets (not random) so both datasets hold the same
# graph. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS INLINE EDGES 16;
	DEFINE TABLE knows TYPE RELATION;

	CREATE |person:1..=10000| RETURN NONE;

	FOR $s IN (SELECT VALUE id FROM person) {
		FOR $k IN array::range(0, 8) {
			LET $t = type::record('person', (record::id($s) * 8 + $k) % 10000 + 1);
			RELATE $s->knows->$t;
		};
	};

	RETURN 'OK';
}
