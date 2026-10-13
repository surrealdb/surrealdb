/**
# Foldable social graph for `graph/combined_social`: the identical
# topology of `graph-social-classic` (2000 vertices × 50 edges) in a
# LIGHTWEIGHT relation, with no inline cache declared so every traversal
# takes the cursor path. The bench's priming pass observes each scope and
# folds it into packed blocks with numeric doc-id entries, so the timed
# statement actually streams block chunks over the record-less relation.
# `run = false`.

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
