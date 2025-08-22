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
value = "NONE"

[[test.results]]
value = "NONE"

*/
DEFINE INDEX index ON TABLE test COUNT;
CREATE test:1;
CREATE test:2;
CREATE test:3;
DELETE test:1;
SELECT COUNT() FROM test EXPLAIN;
SELECT COUNT() FROM test;
