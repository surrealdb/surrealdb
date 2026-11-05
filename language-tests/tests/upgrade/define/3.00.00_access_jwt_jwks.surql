/**
[env]
imports = ["upgrade/define/3.00.00_access_jwt_jwks_import.surql"]

[test]
upgrade = true
reason = """
Covers the Jwks arm of the stored JwtAccessVerify enum. A JWKS URL that was
lost or misread on decode would leave the access unable to verify any token, and
the rendered statement alone cannot distinguish the arm from a Key verifier.

The import writes the variant into all three places it can live: a
namespace-scoped JWT access, a database-scoped JWT access, and the JwtAccess
carried by a record access. Both read paths are asserted, because they are
independent: the DDL string is rebuilt through the AST, while INFO ... STRUCTURE
serialises the catalog struct directly, so a variant that decoded to the wrong
arm shows up in one even if the other still looked right.

A JWKS access with no issuer can verify but not issue, which is why the JWT
access renders no FOR TOKEN duration and reports no issuer.

Token verification itself is deliberately not exercised. Fetching the key set
needs both an outbound request and a session presenting a token, and the runner
connects as root over its own endpoint with no way to sign in."""

[[test.results]]
match = '''
    $result.accesses.up_access_jwt_jwks_ns
        == "DEFINE ACCESS up_access_jwt_jwks_ns ON NAMESPACE TYPE JWT URL 'https://example.com/ns/jwks.json' DURATION FOR SESSION 30m"
'''

[[test.results]]
match = '''
    $result.accesses.up_access_jwt_jwks_db
        == "DEFINE ACCESS up_access_jwt_jwks_db ON DATABASE TYPE JWT URL 'https://example.com/db/jwks.json' DURATION FOR SESSION 2h COMMENT 'jwks verified access'"
    && $result.accesses.up_access_jwt_jwks_rec
        == "DEFINE ACCESS up_access_jwt_jwks_rec ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM up_access_jwt_jwks_user WHERE name = $name) WITH JWT URL 'https://example.com/rec/jwks.json' DURATION FOR TOKEN 1h, FOR SESSION 1h"
'''

[[test.results]]
match = '''
    $result[WHERE $this.name = 'up_access_jwt_jwks_db'][0].kind.jwt.verify.url
        == 'https://example.com/db/jwks.json'
    && $result[WHERE $this.name = 'up_access_jwt_jwks_db'][0].kind.jwt.issuer == NONE
    && $result[WHERE $this.name = 'up_access_jwt_jwks_rec'][0].kind.jwt.verify.url
        == 'https://example.com/rec/jwks.json'
    && $result[WHERE $this.name = 'up_access_jwt_jwks_rec'][0].kind.jwt.issuer == NONE
'''

[[test.results]]
match = '''
    $result[WHERE $this.name = 'up_access_jwt_jwks_ns'][0].kind.jwt.verify.url
        == 'https://example.com/ns/jwks.json'
    && $result[WHERE $this.name = 'up_access_jwt_jwks_ns'][0].kind.jwt.issuer == NONE
'''

*/
INFO FOR NS;
INFO FOR DB;
-- The structured form reads the decoded catalog value rather than re-rendering
-- the statement, so it is what distinguishes a Jwks verifier from a Key one.
RETURN (INFO FOR DB STRUCTURE).accesses;
-- The namespace-scoped access needs its own scope: the database structure
-- cannot see it, so without this its verifier variant would be covered by the
-- rendered string alone.
RETURN (INFO FOR NS STRUCTURE).accesses;
