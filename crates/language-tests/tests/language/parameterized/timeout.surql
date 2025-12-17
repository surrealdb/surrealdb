/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed because it exceeded the timeout: 0ns"

[[test.results]]
error = "The query was not executed because it exceeded the timeout: 0ns"

[[test.results]]
error = "The query was not executed because it exceeded the timeout: 0ns"

[[test.results]]
error = "The query was not executed because it exceeded the timeout: 0ns"

*/
LET $timeout = 0ns;
DEFINE TABLE test;

CREATE |test:1..1000| TIMEOUT $timeout;

SELECT * FROM |test:1..1000| TIMEOUT $timeout;

UPDATE |test:1..1000| TIMEOUT $timeout;

DELETE |test:1..1000| TIMEOUT $timeout;