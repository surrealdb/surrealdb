/**
[test]
wip = true
reason = "Test set::first function for getting the first (minimum) element"

*/

-- First element of numeric set
RETURN set::first(type::set([3, 1, 2]));

-- First element of string set (alphabetically)
RETURN set::first(type::set(['banana', 'apple', 'cherry']));

-- Empty set
RETURN set::first(type::set([]));

-- Single element
RETURN set::first(type::set([1]));
