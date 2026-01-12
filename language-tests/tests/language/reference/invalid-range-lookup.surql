/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot scan a specific range of record references without a referencing field"

*/
DEFINE TABLE a;
a:1<~lookup:1..2;