/**
[test]

[[test.results]]
value = 'NONE'

[[test.results]]
error = "The field 'foo' does not exist"

*/

DEFINE DB test;
REMOVE FIELD foo ON bar;
