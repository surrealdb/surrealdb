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
value = "{ test: 'DEFINE SEQUENCE test BATCH 100 START 0 TIMEOUT 5s' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"

*/
LET $name = "test";
LET $batch = 100;
LET $start = 0;
LET $timeout = 5s;

DEFINE SEQUENCE $name BATCH $batch START $start TIMEOUT $timeout;
(INFO FOR DB).sequences;

REMOVE SEQUENCE $name;
(INFO FOR DB).sequences;