/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test SELECT FROM NONE and NULL returns empty results"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

SELECT * FROM NONE;
SELECT * FROM NULL;
SELECT 'A' FROM NONE;
SELECT 'A' FROM NULL;
SELECT * FROM NONE, NONE;
SELECT * FROM NULL, NULL;
SELECT 'A' FROM NONE, NONE;
SELECT 'A' FROM NULL, NULL;
SELECT * FROM [NONE, NONE];
SELECT * FROM [NULL, NULL];
SELECT 'A' FROM [NONE, NONE];
SELECT 'A' FROM [NULL, NULL];
