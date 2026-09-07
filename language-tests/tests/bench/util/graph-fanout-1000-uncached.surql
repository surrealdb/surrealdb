/**
# The uncapped twin of `graph-fanout-1000-cached`: the same 1000 `person`
# nodes and 800 deterministic `knows` edges, but no `INLINE EDGES` cap, so
# every traversal opens a cursor per (source, direction). Baseline for the
# `graph/fanout_inline_cache` bench. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS;
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
