/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
DEFINE TABLE tab;
DEFINE EVENT ev ON tab WHEN a == "b" THEN bla COMMENT "comment";
