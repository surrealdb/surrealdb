/**
[test]

[[test.results]]
value = 'NONE'

[[test.results]]
error = "The table 'foo' does not exist"

*/
DEFINE DB test;
REMOVE TABLE foo;
