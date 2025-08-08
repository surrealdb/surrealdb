/**
[test]

[[test.results]]
value = 'NONE'

[[test.results]]
error = "The function 'fn::foo' does not exist"

*/

DEFINE DB test;
REMOVE FUNCTION fn::foo;
