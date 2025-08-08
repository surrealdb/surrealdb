/**
[test]

[[test.results]]
value = 'NONE'

[[test.results]]
error = "The analyzer 'foo' does not exist"

*/

DEFINE DB test;
REMOVE ANALYZER foo;
