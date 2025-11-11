/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

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
skip-record-id-key = true

[[test.results]]
value = "[{ a: 2, id: test:g2ojc7en7dunuc3i2ywk }]"
skip-record-id-key = true

[[test.results]]
value = "[{ a: 1, id: test:crc7qde29bwmdde48eyo }, { a: 2, id: test:g2ojc7en7dunuc3i2ywk }]"
skip-record-id-key = true

[[test.results]]
value = "[{ count: 2, id: view:[] }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'test' does not exist"

[[test.results]]
# TODO: Shouldn't this be 0?
value = "[{ count: 2, id: view:[] }]"

*/

DEFINE DATABASE strict_test STRICT;
USE DB strict_test;

DEFINE TABLE test SCHEMAFUL;
DEFINE FIELD a ON test TYPE number;
DEFINE TABLE view AS SELECT count() FROM test GROUP ALL;

SELECT * FROM view;

create test SET a = 1;
create test SET a = 2;

SELECT * FROM test ORDER BY a ASC;
SELECT * FROM view;

REMOVE TABLE test;

SELECT * FROM test ORDER BY a ASC;
SELECT * FROM view;

