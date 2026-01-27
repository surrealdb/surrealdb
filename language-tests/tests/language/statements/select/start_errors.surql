/**
[test]

# 0: No limit
[[test.results]]
error = "Found -1 but the START clause must evaluate to a positive integer"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

# 0: Error Negative limit
SELECT * FROM <array>0..10 START -1;

# 1: Successfully start at u32::MAX
SELECT * FROM <array>0..10 START 4294967295;

# 2: Successfully start at u32::MAX + 1
SELECT * FROM <array>0..10 START 4294967296;
