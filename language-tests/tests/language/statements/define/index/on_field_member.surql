/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD id ON test TYPE {iid: string, parent: number };
DEFINE INDEX test_id ON test FIELDS id.parent;
