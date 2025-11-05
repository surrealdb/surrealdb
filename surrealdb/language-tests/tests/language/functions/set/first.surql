/**
[test]

[[test.results]]
value = "1"

[[test.results]]
value = "'apple'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

*/

-- First element of numeric set
RETURN set::first(type::set([3, 1, 2]));

-- First element of string set (alphabetically)
RETURN set::first(type::set(['banana', 'apple', 'cherry']));

-- Empty set
RETURN set::first(type::set([]));

-- Single element
RETURN set::first(type::set([1]));
