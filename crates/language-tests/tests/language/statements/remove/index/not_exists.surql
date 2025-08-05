/**
[test]


[[test.results]]
value = 'NONE'

[[test.results]]
error = "The index 'foo' does not exist"

*/
DEFINE DB test;
REMOVE INDEX foo ON bar;
