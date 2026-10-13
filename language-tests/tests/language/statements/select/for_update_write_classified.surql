/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]

[test]
reason = "A SELECT ... FOR UPDATE nested inside a value literal makes the enclosing statement write-classified, so it runs on a committing write transaction and the locked read succeeds with its conflict registration armed"

[[test.results]]
value = "[{ id: person:1 }]"

[[test.results]]
value = "[[{ id: person:1 }]]"

*/

CREATE person:1;
RETURN [(SELECT * FROM person:1 FOR UPDATE)];
