/**
[env.capabilities]

[test]
reason = "Test ACCESS SHOW with WHERE filtering"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.ac == 'api' && $result.subject == { user: 'tobie'}"

[[test.results]]
match = "$result.ac == 'api' && $result.subject == { user: 'jaime'}"

[[test.results]]
match = "array::len($result) == 1 && $result[0].subject == { user: 'tobie'}"

[[test.results]]
match = "array::len($result) == 1 && $result[0].subject == { user: 'jaime'}"

[[test.results]]
match = "array::len($result) == 2"

[[test.results]]
match = "array::len($result) == 2 && $result[0].revocation == NONE"

[[test.results]]
match = "array::len($result) == 1 && $result[0][0].revocation != NONE"

[[test.results]]
match = "array::len($result) == 1 && $result[0].revocation != NONE"

*/

DEFINE ACCESS api ON DATABASE TYPE BEARER FOR USER;
DEFINE USER tobie ON DATABASE PASSWORD 'secret' ROLES EDITOR;
DEFINE USER jaime ON DATABASE PASSWORD 'secret' ROLES EDITOR;
ACCESS api GRANT FOR USER tobie;
ACCESS api GRANT FOR USER jaime;
-- Filter by subject
ACCESS api SHOW WHERE subject.user = 'tobie';
ACCESS api SHOW WHERE subject.user = 'jaime';
-- Show all and filter by revocation status
ACCESS api SHOW ALL;
ACCESS api SHOW WHERE revocation IS NONE;
-- Revoke one and show revoked
ACCESS api REVOKE WHERE subject.user = 'tobie';
ACCESS api SHOW WHERE revocation IS NOT NONE;
