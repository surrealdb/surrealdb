/**
[env.capabilities]

[test]
reason = "Test ACCESS REVOKE operations for bearer grants"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.ac == 'api' && $result.subject == { user: 'tobie'} && $result.revocation == NONE"

[[test.results]]
match = "$result.ac == 'api' && $result.subject == { user: 'jaime'} && $result.revocation == NONE"

[[test.results]]
match = "array::len($result) == 1 && array::len($result[0]) == 1 && $result[0][0].subject == { user: 'tobie'} && $result[0][0].revocation != NONE"

[[test.results]]
match = "array::len($result) == 1 && array::len($result[0]) == 1 && $result[0][0].subject == { user: 'jaime'} && $result[0][0].revocation != NONE"

*/

DEFINE ACCESS api ON DATABASE TYPE BEARER FOR USER;
DEFINE USER tobie ON DATABASE PASSWORD 'secret' ROLES EDITOR;
DEFINE USER jaime ON DATABASE PASSWORD 'secret' ROLES EDITOR;
ACCESS api GRANT FOR USER tobie;
ACCESS api GRANT FOR USER jaime;
-- Revoke tobie's grant via WHERE
ACCESS api REVOKE WHERE subject.user = 'tobie';
-- Revoke all remaining
ACCESS api REVOKE ALL;
