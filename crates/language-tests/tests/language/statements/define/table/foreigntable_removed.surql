/**
[test]
wip = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, id: test:crc7qde29bwmdde48eyo }]"

[[test.results]]
value = "[{ a: 2, id: test:g2ojc7en7dunuc3i2ywk }]"

[[test.results]]
value = "[{ a: 1, id: test:crc7qde29bwmdde48eyo }, { a: 2, id: test:g2ojc7en7dunuc3i2ywk }]"

[[test.results]]
value = "[{ count: 2, id: view:[] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2, id: view:[] }]"

*/


DEFINE TABLE test SCHEMAFUL;
DEFINE FIELD a ON test TYPE number;
DEFINE TABLE view AS SELECT count() FROM test GROUP ALL;

SELECT * FROM view;

create test SET a = 1;
create test SET a = 2;

SELECT * FROM test;
SELECT * FROM view;

REMOVE TABLE test;

SELECT * FROM test;
SELECT * FROM view;

