/**
[test]
reason = "Test set::max function for getting the maximum value"

[[test.results]]
value = "3"

[[test.results]]
value = "'cherry'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "5"

[[test.results]]
value = "5"

*/

-- Maximum of numeric set
RETURN set::max(type::set([1, 3, 2]));

-- Maximum of string set (alphabetically)
RETURN set::max(type::set(['banana', 'apple', 'cherry']));

-- Empty set
RETURN set::max(type::set([]));

-- Single element
RETURN set::max(type::set([5]));

-- Set with negative numbers
RETURN set::max(type::set([5, -10, 3, 0]));

