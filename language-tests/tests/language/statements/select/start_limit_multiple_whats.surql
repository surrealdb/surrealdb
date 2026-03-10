/**
[test]

# 0: No limit
[[test.results]]
value = "[0, 1, 2, 3, 4, 5, 6]"

# 1, 2, 3, 4: Limit starting at 0
[[test.results]]
value = "[0]"
[[test.results]]
value = "[0, 1]"
[[test.results]]
value = "[0, 1, 2]"
[[test.results]]
value = "[0, 1, 2, 3, 4, 5]"

# 5, 6, 7, 8: Limit starting at 1
[[test.results]]
value = "[1]"
[[test.results]]
value = "[1, 2]"
[[test.results]]
value = "[1, 2, 3]"
[[test.results]]
value = "[1, 2, 3, 4, 5, 6]"

# 9, 10, 11, 12: Limit overflowing end
[[test.results]]
value = "[4]"
[[test.results]]
value = "[4, 5]"
[[test.results]]
value = "[4, 5, 6]"
[[test.results]]
value = "[4, 5, 6]"

*/


// 0: No limit
SELECT * FROM [0, 1], [2, 3, 4], [5, 6];

// 1, 2, 3, 4: Limit starting at 0
SELECT * FROM [0, 1], [2, 3, 4], [5, 6] START 0 LIMIT 1;
SELECT * FROM [0, 1], [2, 3, 4], [5, 6] START 0 LIMIT 2;
SELECT * FROM [0, 1], [2, 3, 4], [5, 6] START 0 LIMIT 3;
SELECT * FROM [0, 1], [2, 3, 4], [5, 6] START 0 LIMIT 6;

// 5, 6, 7, 8: Limit starting at 1
SELECT * FROM [0, 1], [2, 3, 4], [5, 6] START 1 LIMIT 1;
SELECT * FROM [0, 1], [2, 3, 4], [5, 6] START 1 LIMIT 2;
SELECT * FROM [0, 1], [2, 3, 4], [5, 6] START 1 LIMIT 3;
SELECT * FROM [0, 1], [2, 3, 4], [5, 6] START 1 LIMIT 6;

// 9, 10, 11, 12: Limit overflowing end
SELECT * FROM [0, 1], [2, 3, 4], [5, 6] START 4 LIMIT 1;
SELECT * FROM [0, 1], [2, 3, 4], [5, 6] START 4 LIMIT 2;
SELECT * FROM [0, 1], [2, 3, 4], [5, 6] START 4 LIMIT 3;
SELECT * FROM [0, 1], [2, 3, 4], [5, 6] START 4 LIMIT 6;
