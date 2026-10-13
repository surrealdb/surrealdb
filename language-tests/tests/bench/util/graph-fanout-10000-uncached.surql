/**
# Uncached twin of `graph-fanout-10000-cached`: the identical 10000-source,
# 8-edges-each graph without the `INLINE EDGES` cap, so the traversal pays
# a cursor per (source, direction). Gives the `graph/fanout_10k` bench its
# baseline side. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS;
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
