/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
DEFINE TABLE normal TYPE NORMAL;
DEFINE TABLE any TYPE ANY;
DEFINE TABLE relation TYPE RELATION FROM normal TO any;
DEFINE TABLE relation_none TYPE RELATION;
DEFINE TABLE relation_enforced TYPE RELATION FROM normal TO any ENFORCED;
