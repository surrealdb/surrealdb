/**
[test]
reason = "Test that ES512 algorithm is rejected at definition time on namespace scope"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The ES512 algorithm is not currently supported. Please use ES384 or another supported algorithm"

*/

LET $name = "testac";
LET $key = "foo";

-- ES512 is not supported by the underlying cryptography library.
-- New definitions using ES512 should be rejected with an explicit error.
DEFINE ACCESS $name ON NAMESPACE TYPE JWT ALGORITHM ES512 KEY $key;
