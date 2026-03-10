/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "Range-bounded reference scans require a referencing field"

*/
DEFINE TABLE a;
a:1<~lookup:1..2;