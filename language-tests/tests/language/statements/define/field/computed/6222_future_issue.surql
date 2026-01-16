## https://github.com/surrealdb/surrealdb/issues/6222

/**

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ id: test:test, test1: true, test2: true }"
*/

DEFINE TABLE OVERWRITE test;

DEFINE FIELD OVERWRITE test1 ON test TYPE bool;
DEFINE FIELD OVERWRITE test2 ON test COMPUTED $this.test1;

CREATE ONLY test:test SET test1 = true;
