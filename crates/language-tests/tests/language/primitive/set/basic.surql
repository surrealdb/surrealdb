/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{1, 2, 3}"

[[test.results]]
value = "'set'"

[[test.results]]
value = "{1, 2, 3}"

[[test.results]]
value = "{42,}"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "3"

*/

LET $set = {1, 2, 3};

-- Basic set value
RETURN $set;

-- Check type
RETURN type::of($set);

-- Non-empty set
RETURN {1, 2, 3};

-- Single element set
RETURN {42,};

-- Sets maintain sorted order
RETURN {3, 1, 2} == {1, 2, 3};

-- Different sets are not equal
RETURN {1, 2} == {1, 2, 3};

-- Set length
RETURN $set.len();

