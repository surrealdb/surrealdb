/**
[env]
timeout = 5000

[test]
reason = "Test LIMIT and START with EXPLAIN plans and ORDER BY"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1, 2, 3, 4, 5]"

[[test.results]]
value = "[5, 6, 7, 8, 9]"

[[test.results]]
value = "[8, 9, 10]"

*/
CREATE |item:100| RETURN NONE;

CREATE |item2:100| RETURN NONE;

-- LIMIT/START from array (deterministic)
SELECT * FROM [1,2,3,4,5,6,7,8,9,10] LIMIT 5 START 0;
SELECT * FROM [1,2,3,4,5,6,7,8,9,10] LIMIT 5 START 4;
SELECT * FROM [1,2,3,4,5,6,7,8,9,10] LIMIT 5 START 7;
