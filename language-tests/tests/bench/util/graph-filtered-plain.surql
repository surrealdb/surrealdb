/**
# The plain twin of `graph-filtered-inline`: the same hub vertex and 20000
# scored `knows` edges without the INLINE clause, giving the
# `graph/filtered_inline` bench its record-path baseline. `run = false`.

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

	FOR $s IN (SELECT VALUE id FROM person WHERE record::id(id) = 1) {
		FOR $n IN array::range(1, 20000) {
			LET $t = type::record('person', $n + 1);
			RELATE $s->knows->$t SET score = $n % 1000;
		};
	};

	RETURN 'OK';
}
