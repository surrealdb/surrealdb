/**
[test]
reason = "Validate ml function parsing, not evaluation."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/
DEFINE TABLE person;

SELECT ml::foo<1.0.0>() FROM person;
