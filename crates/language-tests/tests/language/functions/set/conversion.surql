/**
[test]

[[test.results]]
value = "{1, 2, 3}"

[[test.results]]
value = "{1, 2, 3}"

[[test.results]]
value = "{1, 2, 3}"

[[test.results]]
value = "{'bar', 'baz', 'foo'}"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/

-- Create set literal with duplicates (deduplicates)
RETURN {1, 2, 3, 2, 1};

-- Create set literal with duplicates (different order)
RETURN {3, 1, 2, 1, 3, 2};

-- Create set from array using type::set
RETURN type::set([1, 2, 3]);

-- Create set from string array
RETURN type::set(['foo', 'bar', 'baz', 'foo']);

-- Type checking
RETURN type::is_set({1, 2, 3});
RETURN type::is_set({,});
RETURN type::is_set([1, 2, 3]);
