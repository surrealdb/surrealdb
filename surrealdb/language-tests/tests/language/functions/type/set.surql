/**
[test]
wip = true
reason = "Set literals are not yet supported"
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
