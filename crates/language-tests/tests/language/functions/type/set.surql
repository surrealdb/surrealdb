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

[[test.results]]
value = "{1, 2}"

[[test.results]]
value = "{1, 2}"

[[test.results]]
value = "{1, 2}"

[[test.results]]
error = "Expected `set<int,5>` but found a collection of length `2`"

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

-- Set cast
RETURN <set>[1,2,1];

-- Set cast with type
RETURN <set<int>>[1,2,1];

-- Set cast with type and length
RETURN <set<int,2>>[1,2,1];

-- Set cast with length failure
RETURN <set<int,5>>[1,2,1];