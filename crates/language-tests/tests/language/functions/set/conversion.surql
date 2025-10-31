/**
[test]
wip = true
reason = "Set literals are not yet supported"
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
