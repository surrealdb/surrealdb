/**
[env.capabilities]

[env]
timeout = 10000

[test]
reason = "REVOKE and PURGE walk a set of BEARER grants from issued, through revoked, to purged"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = ''' $result.ac == 'api' && $result.revocation == NONE '''

[[test.results]]
match = ''' $result.ac == 'api' && $result.revocation == NONE '''

[[test.results]]
match = ''' $result == [[]] '''

[[test.results]]
match = '''
	array::len($result) == 1
		&& array::len($result[0]) == 2

		&& $result[0][0].ac == 'api'
		&& $result[0][0].grant.key == '[REDACTED]'
		&& $result[0][0].revocation != NONE

		&& $result[0][1].ac == 'api'
		&& $result[0][1].grant.key == '[REDACTED]'
		&& $result[0][1].revocation != NONE
'''

[[test.results]]
match = ''' $result == [] '''

[[test.results]]
match = ''' $result == [[]] '''

[[test.results]]
match = ''' $result == [] '''

[[test.results]]
match = ''' $result == [] '''

[[test.results]]
match = ''' $result == [] '''

[[test.results]]
value = "NONE"

[[test.results]]
match = '''
	array::len($result) == 2

		&& $result[0].ac == 'api'
		&& $result[0].grant.key == '[REDACTED]'
		&& $result[0].revocation != NONE

		&& $result[1].ac == 'api'
		&& $result[1].grant.key == '[REDACTED]'
		&& $result[1].revocation != NONE
'''

[[test.results]]
match = ''' $result == [] '''

[[test.results]]
error = "The access method 'invalid' does not exist in the database 'test'"

[[test.results]]
error = "The access method 'invalid' does not exist in the database 'test'"

[[test.results]]
error = "The access grant 'invalid' does not exist for 'api' in the database 'test'"
*/


-- Initial setup
DEFINE ACCESS api ON DATABASE TYPE BEARER FOR USER;
DEFINE USER tobie ON DATABASE PASSWORD 'secret' ROLES EDITOR;
ACCESS api ON DATABASE GRANT FOR USER tobie;
ACCESS api ON DATABASE GRANT FOR USER tobie;
-- A condition matching no grant revokes nothing
ACCESS api ON DATABASE REVOKE WHERE revocation != NONE;
-- A condition matching every grant revokes all of them
ACCESS api ON DATABASE REVOKE WHERE revocation == NONE;
ACCESS api ON DATABASE SHOW WHERE revocation == NONE;
-- An already revoked grant is not revoked again
ACCESS api ON DATABASE REVOKE ALL;
-- A grace period longer than the age of the revocation purges nothing
ACCESS api ON DATABASE PURGE REVOKED FOR 1h;
ACCESS api ON DATABASE PURGE EXPIRED, REVOKED FOR 1d;
ACCESS api ON DATABASE PURGE REVOKED EXPIRED FOR 1d;
-- Without a grace period the revoked grants are purged
SLEEP 1100ms;
ACCESS api ON DATABASE PURGE REVOKED;
ACCESS api ON DATABASE SHOW ALL;
-- An access method that does not exist cannot be revoked from or purged
ACCESS invalid ON DATABASE REVOKE ALL;
ACCESS invalid ON DATABASE PURGE EXPIRED;
-- Should fail
ACCESS api ON DATABASE REVOKE GRANT invalid;
