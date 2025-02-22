/**
[test]

[[test.results]]
value = "false"

[[test.results]]
value = "{ id: person:tobie }"

[[test.results]]
value = "true"

*/
record::exists(r"person:tobie");
CREATE ONLY person:tobie;
record::exists(r"person:tobie");
