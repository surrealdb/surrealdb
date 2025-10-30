/**
[test]
wip = true
reason = "Test set::flatten function for flattening nested sets and arrays"

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

