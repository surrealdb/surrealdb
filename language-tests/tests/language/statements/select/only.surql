/**
[test]

# 0: Test ONLY with a single value
[[test.results]]
value = "1"

# 1: Test ONLY with NONE
[[test.results]]
value = "NONE"

# 2: Test ONLY with an empty array
[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

# 3: Test ONLY with a single element array
[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

# 4: Test ONLY with a two element array
[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

# 5: Test ONLY with an empty array and a limit
[[test.results]]
value = "NONE"

# 6: Test ONLY with a single element array and a limit
[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
value = "1"

*/
// 0: Test ONLY with a single value
SELECT * FROM ONLY 1;

// 1: Test ONLY with NONE
SELECT * FROM ONLY NONE;

// 2: Test ONLY with an empty array
SELECT * FROM ONLY [];

// 3: Test ONLY with a single element array
SELECT * FROM ONLY [1];

// 4: Test ONLY with a two element array
SELECT * FROM ONLY [1, 2];

// 5: Test ONLY with an empty array and a limit
SELECT * FROM ONLY [] LIMIT 1;

// 6: Test ONLY with a single element array and a limit
SELECT * FROM ONLY [1] LIMIT 1;

// 7: Test ONLY with a two element array and a limit
SELECT * FROM ONLY [1, 2] LIMIT 1;

// 8: Test ONLY with multiple tables should error
SELECT * FROM ONLY 1, 2;

// 9: Test with multiple tables and a limit
SELECT * FROM ONLY 1, 2 LIMIT 1;
