/**
[test]
reason = "Test that ES512 algorithm is rejected at definition time when used as issuer algorithm with JWKS verify on namespace"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The ES512 algorithm is not currently supported. Please use ES384 or another supported algorithm"

*/

LET $name = "testac";
LET $issuer_key = "bar";

-- ES512 is not supported by the underlying cryptography library.
-- When using JWKS for verification, the issuer algorithm is specified separately.
-- New definitions using ES512 as the issuer algorithm should be rejected.
DEFINE ACCESS $name ON NAMESPACE TYPE JWT URL "http://example.com/.well-known/jwks.json" WITH ISSUER ALGORITHM ES512 KEY $issuer_key;
