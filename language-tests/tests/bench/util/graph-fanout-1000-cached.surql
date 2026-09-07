/**
# Fan-out dataset for the inline adjacency cache bench: 1000 `person` nodes
# where the first 100 each have 8 outgoing `knows` edges to deterministic
# targets. The vertex table carries `INLINE EDGES 16`, so the RELATEs
# materialise every source's cache — the `graph/fanout_inline_cache` bench
# then measures a 100-source fan-out served entirely from cache hits.
# `graph-fanout-1000-uncached` is the byte-identical twin without the cap,
# giving the bench its baseline. Deterministic targets (not random) so both
# datasets hold the same graph. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS INLINE EDGES 16;
	DEFINE TABLE knows TYPE RELATION;

	CREATE |person:1..=1000| SET name = rand::string(12) RETURN NONE;

	FOR $s IN (SELECT VALUE id FROM person WHERE record::id(id) <= 100) {
		FOR $k IN array::range(0, 8) {
			LET $t = type::record('person', (record::id($s) * 8 + $k) % 1000 + 1);
			RELATE $s->knows->$t;
		};
	};

	RETURN 'OK';
}
