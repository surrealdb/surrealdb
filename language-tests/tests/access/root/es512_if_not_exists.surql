/**
[env]
clean = true

[test]
reason = "Test that DEFINE ACCESS IF NOT EXISTS with ES512 does not error when the access already exists"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

LET $key = "foo";

-- First, define an access method with a supported algorithm.
DEFINE ACCESS testac ON ROOT TYPE JWT ALGORITHM HS256 KEY $key;

-- IF NOT EXISTS should no-op since the access already exists, even if ES512 is specified.
DEFINE ACCESS IF NOT EXISTS testac ON ROOT TYPE JWT ALGORITHM ES512 KEY $key;
