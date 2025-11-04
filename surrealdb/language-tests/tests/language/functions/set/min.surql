/**
[test]
reason = "Test set::min function for getting the minimum value"

[[test.results]]
value = "1"

[[test.results]]
value = "'apple'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "5"

[[test.results]]
value = "-10"

*/

-- Minimum of numeric set
RETURN set::min(type::set([3, 1, 2]));

-- Minimum of string set (alphabetically)
RETURN set::min(type::set(['banana', 'apple', 'cherry']));

-- Empty set
RETURN set::min(type::set([]));

-- Single element
RETURN set::min(type::set([5]));

-- Set with negative numbers
RETURN set::min(type::set([5, -10, 3, 0]));

