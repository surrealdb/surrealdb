/**
[test]

[[test.results]]
value = 'NONE'

[[test.results]]
error = "The access method 'foo' does not exist in the database 'test'"

*/

DEFINE DB test;
REMOVE ACCESS foo ON DB;
