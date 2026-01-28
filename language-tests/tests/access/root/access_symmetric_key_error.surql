/**
[test]
reason = "Test that symmetric algorithms reject different keys for signing and verification"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: Symmetric algorithm HS256 requires the same key for signing and verification. Use the same key value for both KEY and WITH ISSUER KEY clauses, or omit WITH ISSUER KEY."

*/

LET $name = "testac";
LET $key = "foo";
LET $issuer_key = "bar";
LET $comment = "test comment";

-- This should fail at runtime because symmetric algorithm HS256 requires the same key
-- for both signing (issuer) and verification
DEFINE ACCESS $name ON NAMESPACE TYPE JWT ALGORITHM HS256 KEY $key WITH ISSUER KEY $issuer_key COMMENT $comment;
