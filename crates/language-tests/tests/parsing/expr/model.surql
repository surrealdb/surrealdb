/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/
DEFINE TABLE person;
# TODO: If I change this to `RETURN ml::foo<1.0.0>()` I get "ML computation is not enabled.". Why?
SELECT ml::foo<1.0.0>() FROM person;
