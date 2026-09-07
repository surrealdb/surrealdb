/**
# Filtered-traversal dataset for the inline-property bench: one hub
# `person` vertex with 20000 outgoing `knows` edges whose `score` is
# INLINE, ~1% of them matching `score > 989`. The plain twin
# (`graph-filtered-plain`) holds the same graph without the INLINE clause,
# so the pair isolates the record fetches a payload-served predicate
# avoids. Deterministic scores so both datasets select identically.
# `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS;
	DEFINE TABLE knows TYPE RELATION;
	DEFINE FIELD score ON knows TYPE number INLINE;

	CREATE |person:1..=20001| SET name = rand::string(8) RETURN NONE;

	FOR $s IN (SELECT VALUE id FROM person WHERE record::id(id) = 1) {
		FOR $n IN array::range(1, 20000) {
			LET $t = type::record('person', $n + 1);
			RELATE $s->knows->$t SET score = $n % 1000;
		};
	};

	RETURN 'OK';
}
