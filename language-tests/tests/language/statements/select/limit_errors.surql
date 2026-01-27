/**
[test]

# 0: No limit
[[test.results]]
error = "Found -1 but the LIMIT clause must evaluate to an integer between 0 and u32::MAX"

[[test.results]]
value = "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]"

[[test.results]]
error = "Found 4294967296 but the LIMIT clause must evaluate to an integer between 0 and u32::MAX"
*/

# 0: Error Negative limit
SELECT * FROM <array>0..10 LIMIT -1;

# 1: Successfully limit to the maximum value
SELECT * FROM <array>0..10 LIMIT 4294967295;

# 2: Error Limit too large
SELECT * FROM <array>0..10 LIMIT 4294967296;
