/**
# Lightweight twin of `graph-social-classic`: the identical 2000-vertex,
# 100000-edge topology in a record-less LIGHTWEIGHT relation — each edge is
# two vertex-side adjacency keys and no record. Gives the
# `graph/social_traverse` and `graph/social_scan` benches their
# record-less side. See the classic twin for why the targets must be
# deterministic and collision-free. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS;
	DEFINE TABLE knows TYPE RELATION IN person OUT person LIGHTWEIGHT;

	CREATE |person:1..=2000| RETURN NONE;

	FOR $s IN (SELECT VALUE id FROM person) {
		FOR $k IN array::range(1, 50) {
			LET $t = type::record('person', (record::id($s) * 53 + $k * 101) % 2000 + 1);
			RELATE $s->knows->$t;
		};
	};

	RETURN 'OK';
}
