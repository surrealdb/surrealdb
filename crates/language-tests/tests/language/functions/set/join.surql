/**
[test]
reason = "Test set::join function for joining set elements into a string"

[[test.results]]
value = "'1, 2, 3'"

[[test.results]]
value = "'apple-banana-cherry'"

[[test.results]]
value = "''"

[[test.results]]
value = "'1'"

[[test.results]]
value = "'1,2,3,4,5'"

*/

-- Join numeric set with comma-space
RETURN set::join(type::set([3, 1, 2]), ', ');

-- Join string set with dash
RETURN set::join(type::set(['banana', 'apple', 'cherry']), '-');

-- Empty set
RETURN set::join(type::set([]), ', ');

-- Single element
RETURN set::join(type::set([1]), ', ');

-- Join with no separator
RETURN set::join(type::set([5, 1, 3, 2, 4]), ',');

