/**
[test]

[[test.results]]
value = "{2, 3}"

[[test.results]]
value = "{1, 2}"

[[test.results]]
value = "{3, 4, 5}"

[[test.results]]
value = "{1, 2, 3, 4, 5}"

[[test.results]]
value = "{4, 5}"

[[test.results]]
value = "{1, 2, 3}"

[[test.results]]
value = "{,}"

[[test.results]]
value = "{2, 3, 4}"

*/

-- Slice middle elements
RETURN set::slice(type::set([1, 2, 3, 4, 5]), 1, 3);

-- Slice from beginning
RETURN set::slice(type::set([1, 2, 3, 4, 5]), 0, 2);

-- Slice to end
RETURN set::slice(type::set([1, 2, 3, 4, 5]), 2, 10);

-- Slice with no end (take all from start)
RETURN set::slice(type::set([1, 2, 3, 4, 5]), 0);

-- Slice with negative start
RETURN set::slice(type::set([1, 2, 3, 4, 5]), -2, 10);

-- Slice with negative end
RETURN set::slice(type::set([1, 2, 3, 4, 5]), 0, -2);

-- Empty result
RETURN set::slice(type::set([1, 2, 3, 4, 5]), 5, 10);

-- Slice using range syntax
RETURN set::slice(type::set([1, 2, 3, 4, 5]), 1..4);
