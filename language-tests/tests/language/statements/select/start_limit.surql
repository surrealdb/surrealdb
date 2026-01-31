/**
[test]

# 0: No limit
[[test.results]]
value = "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]"

# 1, 2, 3: Limit
[[test.results]]
value = "[]"
[[test.results]]
value = "[0]"
[[test.results]]
value = "[0, 1]"

# 4, 5, 6: Limit & Start
[[test.results]]
value = "[]"
[[test.results]]
value = "[1]"
[[test.results]]
value = "[1, 2]"

# 7, 8, 9: Limit & Start overflowing the end
[[test.results]]
value = "[]"
[[test.results]]
value = "[8]"
[[test.results]]
value = "[8, 9]"

# 10, 11, 12: Limit & Start starting past the end
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"

*/

# 0: No limit
SELECT * FROM <array>0..10;

# 1, 2, 3: Limit
SELECT * FROM <array>0..10 LIMIT 0;
SELECT * FROM <array>0..10 LIMIT 1;
SELECT * FROM <array>0..10 LIMIT 2;

# 4, 5, 6: Limit & Start
SELECT * FROM <array>0..10 LIMIT 0 START 1;
SELECT * FROM <array>0..10 LIMIT 1 START 1;
SELECT * FROM <array>0..10 LIMIT 2 START 1;

# 7, 8, 9: Limit & Start overflowing the end
SELECT * FROM <array>0..10 LIMIT 0 START 8;
SELECT * FROM <array>0..10 LIMIT 1 START 8;
SELECT * FROM <array>0..10 LIMIT 2 START 8;

# 10, 11, 12: Limit & Start starting past the end
SELECT * FROM <array>0..10 LIMIT 0 START 10;
SELECT * FROM <array>0..10 LIMIT 1 START 10;
SELECT * FROM <array>0..10 LIMIT 2 START 10;
