/**
[test]
reason = "Test set::last function for getting the last (maximum) element"

[[test.results]]
value = "3"

[[test.results]]
value = "'cherry'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

*/

-- Last element of numeric set
RETURN set::last(type::set([1, 3, 2]));

-- Last element of string set (alphabetically)
RETURN set::last(type::set(['banana', 'apple', 'cherry']));

-- Empty set
RETURN set::last(type::set([]));

-- Single element
RETURN set::last(type::set([1]));

