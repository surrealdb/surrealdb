## https://github.com/surrealdb/surrealdb/issues/4899

/**

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ id: A:1, test: NULL }"

[[test.results]]
value = "{ id: A:2, test: NULL }"

[[test.results]]
value = "[{ id: A:1, test: NULL }, { id: A:2, test: NULL }]"
*/


DEFINE TABLE OVERWRITE test;

DEFINE FIELD test ON TABLE A COMPUTED NULL;
CREATE ONLY A:1 SET test = null;
CREATE ONLY A:2;
SELECT * FROM A WHERE test = null;