/**
[test]

# 0
[[test.results]]
value = "[1, 2, 3]"

# 1: [0..1]
[[test.results]]
value = "[1]"

# 2: [0..=1]
[[test.results]]
value = "[1, 2]"

# 3: [1>..=2]
[[test.results]]
value = "[3]"

# 4: [1>..]
[[test.results]]
value = "[3]"

# 5: [1..]
[[test.results]]
value = "[2, 3]"

# 6: [..2]
[[test.results]]
value = "[1, 2]"

# 7: [..=2]
[[test.results]]
value = "[1, 2, 3]"

# 8: Minimizing a single element array returns the element.
[[test.results]]
value = "1"

# 9: Minimizing a single element array in a nested array returns the element.
[[test.results]]
value = "[3, 1]"

*/

-- 0: Original array.
[1, 2, 3];

-- Indexing an array with a range.
-- 1
[1, 2, 3][0..1];
-- 2
[1, 2, 3][0..=1];
-- 3
[1, 2, 3][1>..=2];
-- 4
[1, 2, 3][1>..];
-- 5
[1, 2, 3][1..];
-- 6
[1, 2, 3][..2];
-- 7
[1, 2, 3][..=2];

-- 8: Minimizing a single element array returns the element.
[1, 2, 3][0..1].min();

-- 9: Minimizing a single element array in a nested array returns the element.
[
	[3, 2, 1],
	[1, 2, 3]
].*[0..1].min();
