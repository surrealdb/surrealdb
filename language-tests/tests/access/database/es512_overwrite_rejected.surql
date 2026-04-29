/**
[test]
reason = "Test that DEFINE ACCESS OVERWRITE with ES512 is rejected when the existing definition does not use ES512"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The ES512 algorithm is not currently supported. Please use ES384 or another supported algorithm"

*/

LET $key = "foo";

-- First, define an access method with a supported algorithm.
DEFINE ACCESS testac ON DATABASE TYPE JWT ALGORITHM HS256 KEY $key;

-- OVERWRITE with ES512 on a non-ES512 definition should be rejected.
DEFINE ACCESS OVERWRITE testac ON DATABASE TYPE JWT ALGORITHM ES512 KEY $key;
