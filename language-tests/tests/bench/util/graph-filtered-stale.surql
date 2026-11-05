/**
# Fallback-heavy twin of the filtered-traversal dataset: the same hub
# `person` vertex with 20000 outgoing `knows` edges and deterministic
# scores, but `score` becomes INLINE only after every edge is written.
# No edge carries a payload, so a payload-admitted predicate falls back
# to the records for the whole hub — the worst case the per-flush bulk
# prefetch exists for. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS;
	DEFINE TABLE knows TYPE RELATION;
	DEFINE FIELD score ON knows TYPE number;

	CREATE |person:1..=20001| SET name = rand::string(8) RETURN NONE;

	FOR $n IN 1..=20000 {
		LET $t = type::record('person', $n + 1);
		RELATE person:1->knows->$t SET score = $n % 1000;
	};

	-- Inline the field only after the edges exist: every payload is
	-- absent, so the filtered traversal answers all 20000 edges from
	-- their records.
	DEFINE FIELD OVERWRITE score ON knows TYPE number INLINE;

	RETURN 'OK';
}
