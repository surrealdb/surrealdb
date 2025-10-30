/**
[test]
wip = true
reason = "Set literals are not yet supported"
*/

LET $set = {1, 2, 3};

-- Basic set value
RETURN $set;

-- Check type
RETURN type::of($set);

-- Non-empty set
RETURN {1, 2, 3};

-- Single element set
RETURN {42};

-- Sets maintain sorted order
RETURN {3, 1, 2} == {1, 2, 3};

-- Different sets are not equal
RETURN {1, 2} == {1, 2, 3};

-- Set length
RETURN $set.len();

