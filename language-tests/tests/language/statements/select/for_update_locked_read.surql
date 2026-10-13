/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]

[test]
reason = "SELECT ... FOR UPDATE routes record-id targets (literal and param) through the locked read and returns the record on an engine with locked-read support"

[[test.results]]
value = "[{ id: person:1, value: 1 }]"

[[test.results]]
value = "[{ id: person:1, value: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, value: 1 }]"

*/

CREATE person:1 SET value = 1;
SELECT * FROM person:1 FOR UPDATE;
LET $r = person:1;
SELECT * FROM $r FOR UPDATE;
