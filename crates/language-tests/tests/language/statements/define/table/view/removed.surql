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
value = "[{ a: 1, id: test:1 }]"

[[test.results]]
value = "[{ a: 2, id: test:2 }]"

[[test.results]]
value = "[{ a: 1, id: test:1 }, { a: 2, id: test:2 }]"

[[test.results]]
value = "[{ count: 2, id: view:[] }]"

[[test.results]]
error = "Invalid query: Cannot delete table `test` on which a view is defined, table(s) `view` are defined as a view on this table."

[[test.results]]
value = "[{ a: 1, id: test:1 }, { a: 2, id: test:2 }]"

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

create test:1 SET a = 1;
create test:2 SET a = 2;

SELECT * FROM test ORDER BY a ASC;
SELECT * FROM view;

REMOVE TABLE test;

SELECT * FROM test ORDER BY a ASC;
SELECT * FROM view;

