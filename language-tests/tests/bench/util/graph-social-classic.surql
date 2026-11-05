/**
# Read-side social graph: 2000 `person` vertices, each with 50 outgoing
# `knows` edges — 100000 edges total — into a classic (record-backed)
# relation. The lightweight twin (`graph-social-lightweight`) holds the
# identical topology in a record-less LIGHTWEIGHT relation, so the
# `graph/social_traverse` and `graph/social_scan` benches isolate what the
# per-edge record costs a traversal and a table scan. Targets are
# deterministic arithmetic (`gcd(101, 2000) = 1` keeps each source's 50
# targets distinct), never random: a random draw could produce duplicate
# (source, target) pairs, which a classic relation stores as two edges but
# a lightweight relation collapses into one canonical id — the twins would
# no longer measure the same work. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS;
	DEFINE TABLE knows TYPE RELATION IN person OUT person;

	CREATE |person:1..=2000| RETURN NONE;

	FOR $s IN (SELECT VALUE id FROM person) {
		FOR $k IN array::range(1, 50) {
			LET $t = type::record('person', (record::id($s) * 53 + $k * 101) % 2000 + 1);
			RELATE $s->knows->$t;
		};
	};

	RETURN 'OK';
}
