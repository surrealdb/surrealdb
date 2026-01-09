/**
[test]

[[test.results]]
value = "{NONE,}"

[[test.results]]
value = "{NULL,}"

[[test.results]]
value = "{NONE, NULL}"

[[test.results]]
value = "{NONE,}"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "{,}"

[[test.results]]
value = "{,}"

*/

-- Set with NONE
RETURN {NONE, NONE};

-- Set with NULL
RETURN {NULL, NULL};

-- Set with NONE and NULL (distinct values)
RETURN {NONE, NULL, NONE, NULL};

-- Add NONE to set
RETURN set::add({,}, NONE);

-- Contains NONE
RETURN set::contains({NONE, NULL}, NONE);

-- Contains NULL
RETURN set::contains({NONE, NULL}, NULL);

-- Complement with self should be empty
RETURN set::complement({1,2,3}, {1,2,3});

-- Intersect with empty set
RETURN set::intersect({1,2,3}, {,});
