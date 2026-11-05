/**
[test]
reason = "Setup for access_jwt_jwks.surql: the old binary writes the three JWKS-verified access definitions whose stored bytes the new one has to decode."
run = false
*/
-- A JWKS URL is only dereferenced when a token is verified, so storing one
-- requires no network capability. These statements define access methods and
-- verify no token, and therefore run under the runner's default capabilities.
DEFINE ACCESS up_access_jwt_jwks_ns ON NAMESPACE TYPE JWT
    URL 'https://example.com/ns/jwks.json'
    DURATION FOR SESSION 30m;
DEFINE ACCESS up_access_jwt_jwks_db ON DATABASE TYPE JWT
    URL 'https://example.com/db/jwks.json'
    DURATION FOR SESSION 2h
    COMMENT 'jwks verified access';
-- The same JwtAccess value also hangs off a record access, which is a second
-- stored home for the JWKS variant.
DEFINE ACCESS up_access_jwt_jwks_rec ON DATABASE TYPE RECORD
    SIGNIN ( SELECT * FROM up_access_jwt_jwks_user WHERE name = $name )
    WITH JWT URL 'https://example.com/rec/jwks.json'
    DURATION FOR SESSION 1h;
