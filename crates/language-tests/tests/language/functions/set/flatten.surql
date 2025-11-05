/**
[test]

[[test.results]]
value = "{1, 2, 3, 4, 5, 6}"

[[test.results]]
value = "{1, 2, 3}"

[[test.results]]
value = "{,}"

[[test.results]]
value = "{1, 2, 3, 4}"

[[test.results]]
value = "{'a', 'b', 'c', 'd'}"

*/

-- Flatten set containing nested sets
RETURN set::flatten(type::set([type::set([1, 2]), type::set([3, 4]), type::set([5, 6])]));

-- Flatten set containing nested arrays
RETURN set::flatten(type::set([[1, 2], [2, 3], [3]]));

-- Empty set
RETURN set::flatten(type::set([]));

-- Mixed nested structures
RETURN set::flatten(type::set([type::set([1, 2]), [3, 4]]));

-- Flatten with strings
RETURN set::flatten(type::set([['a', 'b'], type::set(['c', 'd'])]));

