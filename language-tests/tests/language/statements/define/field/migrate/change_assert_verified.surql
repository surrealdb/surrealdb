/**
[test]
reason = "Change 17: an ASSERT added or changed on an enforced table with rows is accepted and queues a verification of the field, which judges each stored value against the ASSERT and counts the rows that violate it in INFO. The rewrite job does not run under the test runner, so what is observable is that a verification of the field is queued and counts nothing yet; the job's judgement is covered by the table migration job tests."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['a', 'b']"

[[test.results]]
value = "[0, 0]"

[[test.results]]
value = "[false, false]"

[[test.results]]
value = "3"

[[test.results]]
value = "[{ a: 1, b: 1, id: p:1 }, { a: 5, b: 2, id: p:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE int;
DEFINE FIELD b ON p TYPE int ASSERT $value > 0;
CREATE p:1 SET a = 1, b = 1 RETURN NONE;
CREATE p:2 SET a = 5, b = 2 RETURN NONE;
-- p:1 violates both new assertions.
ALTER FIELD a ON p ASSERT $value > 3;
ALTER FIELD b ON p ASSERT $value > 1;
(INFO FOR TABLE p).migrations.verification.*.field;
(INFO FOR TABLE p).migrations.verification.*.failures;
(INFO FOR TABLE p).migrations.verification.*.finished;
(INFO FOR TABLE p).migrations.generation;
-- Nothing else changed.
SELECT * FROM p ORDER BY id;
-- A table with no rows has nothing to verify.
DEFINE TABLE e SCHEMAFULL;
DEFINE FIELD a ON e TYPE int;
ALTER FIELD a ON e ASSERT $value > 3;
(INFO FOR TABLE e).migrations.verification;
