/**
[env.capabilities]

[test]
reason = "Test ACCESS PURGE operations for revoked grants"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.ac == 'api' && $result.subject == { user: 'tobie'}"

[[test.results]]
match = "array::len($result) == 1 && $result[0][0].revocation != NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

DEFINE ACCESS api ON DATABASE TYPE BEARER FOR USER DURATION FOR GRANT 365d;
DEFINE USER tobie ON DATABASE PASSWORD 'secret' ROLES EDITOR;
ACCESS api GRANT FOR USER tobie;
-- Revoke, then purge with grace (nothing removed), then purge without
ACCESS api REVOKE ALL;
ACCESS api PURGE REVOKED FOR 365d;
ACCESS api PURGE REVOKED;
-- Exercise purge both kinds
ACCESS api PURGE EXPIRED, REVOKED;
