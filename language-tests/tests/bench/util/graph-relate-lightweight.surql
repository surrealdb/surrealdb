/**
# 1000 `person` vertices and a lightweight `knows` relation for the
# `graph/relate_lightweight` RELATE-throughput bench. The classic twin
# (`graph-relate-classic`) differs only in the relation's flags, so the
# pair isolates the per-edge write cost of the record-less form.
# `rebuild = true` on the bench keeps each sample against a fresh store.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS;
	DEFINE TABLE knows TYPE RELATION IN person OUT person LIGHTWEIGHT;

	CREATE |person:1..=1000| SET name = rand::string(12) RETURN NONE;

	RETURN 'OK';
}
