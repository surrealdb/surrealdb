/**
[test]

[[test.results]]
value = "{,}"

[[test.results]]
value = "[{1, 2, 3}, {1, 2, 3}]"

[[test.results]]
value = "[{1, 2, 3}, {1, 2, 3}]"

[[test.results]]
value = "[{1, 2, 3}, {1, 2, 3}]"

[[test.results]]
value = "[{'bar', 'foo'}, {'bar', 'foo'}]"

[[test.results]]
error = "Could not cast into `set` using input `3`"

*/

-- Empty set
RETURN {,};

-- Simple set
RETURN [{1, 2, 3}, type::set([1, 2, 3])];

-- Set deduplicates values
RETURN [{1, 2, 3, 2, 1}, type::set([1, 2, 3, 2, 1])];

-- Set from array with many duplicates
RETURN [{1, 1, 1, 2, 2, 3, 3, 3}, type::set([1, 1, 1, 2, 2, 3, 3, 3])];

-- Set automatically sorts (string comparison)
RETURN [{'foo', 'bar', 'foo', 'bar'}, type::set(['foo', 'bar', 'foo', 'bar'])];

-- Cannot convert non-array to set
RETURN type::set(3);
