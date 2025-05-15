/**
[test]

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
value = "1"

*/
SELECT * FROM ONLY 1;
SELECT * FROM ONLY NONE;
SELECT * FROM ONLY [];
SELECT * FROM ONLY [1];
SELECT * FROM ONLY [1, 2];
SELECT * FROM ONLY [] LIMIT 1;
SELECT * FROM ONLY [1] LIMIT 1;
SELECT * FROM ONLY [1, 2] LIMIT 1;
SELECT * FROM ONLY 1, 2;
SELECT * FROM ONLY 1, 2 LIMIT 1;
