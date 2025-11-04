/**
[test]

[[test.results]]
value = "[9]"

[[test.results]]
value = "9"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

*/
-- Even this returns an array
SELECT * FROM 9;
-- Use the `ONLY` clause to return a single item
SELECT * FROM ONLY 9;
-- Error: array has more than one item
SELECT * FROM ONLY [1,9];
