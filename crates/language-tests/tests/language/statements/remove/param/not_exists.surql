/**
[test]

[[test.results]]
value = 'NONE'

[[test.results]]
error = "The param '$foo' does not exist"

*/

DEFINE DB test;
REMOVE PARAM $foo;
