/**
[test]

[[test.results]]
error = "The event 'foo' does not exist"

[[test.results]]
value = 'NONE'

[[test.results]]
error = "The event 'foo' does not exist"

*/
REMOVE EVENT foo ON bar;

DEFINE TABLE bar;
REMOVE EVENT foo ON bar;
