/**
# The classic twin of `graph-relate-lightweight`: the same 1000 `person`
# vertices with an ENFORCED (record-bearing) `knows` relation, giving the
# `graph/relate_lightweight` bench its baseline. `rebuild = true` on the
# bench keeps each sample against a fresh store.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS;
	DEFINE TABLE knows TYPE RELATION IN person OUT person ENFORCED;

	CREATE |person:1..=1000| SET name = rand::string(12) RETURN NONE;

	RETURN 'OK';
}
