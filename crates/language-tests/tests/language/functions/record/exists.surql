/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "false"

[[test.results]]
value = "{ id: person:tobie }"

[[test.results]]
value = "true"

*/
DEFINE TABLE person;
record::exists(r"person:tobie");
CREATE ONLY person:tobie;
record::exists(r"person:tobie");
