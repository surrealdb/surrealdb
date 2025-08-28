/**
[env]
database = false

[env.capabilities]
allow-experimental = ["bearer_access"]

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = ''' $result.accesses.api == 'DEFINE ACCESS api ON NAMESPACE TYPE BEARER FOR USER DURATION FOR GRANT 4w2d, FOR TOKEN 1h, FOR SESSION NONE' '''

[[test.results]]
match = '''
	$result.ac == 'api'
		&& 'creation' in $result
		&& 'expiration' in $result
		&& type::is::object($result.grant)
		&& $result.revocation == NONE
		&& $result.subject == { user: 'tobie'}
		&& $result.type == 'bearer'
'''

[[test.results]]
match = '''
	$result.ac == 'api'
		&& 'creation' in $result
		&& 'expiration' in $result
		&& type::is::object($result.grant)
		&& $result.revocation == NONE
		&& $result.subject == { user: 'tobie'}
		&& $result.type == 'bearer'
'''

[[test.results]]
match = '''
	$result.ac == 'api'
		&& 'creation' in $result
		&& 'expiration' in $result
		&& type::is::object($result.grant)
		&& $result.revocation == NONE
		&& $result.subject == { user: 'tobie'}
		&& $result.type == 'bearer'
'''

[[test.results]]
match = '''
	$result.ac == 'api'
		&& 'creation' in $result
		&& 'expiration' in $result
		&& type::is::object($result.grant)
		&& $result.revocation == NONE
		&& $result.subject == { user: 'tobie'}
		&& $result.type == 'bearer'
'''

[[test.results]]
match = '''
	array::len($result) == 4

		&& $result[0].ac == 'api'
		&& type::is::object($result[0].grant)
		&& $result[0].grant.key = '[REDACTED]'
		&& 'id' in $result[0].grant

		&& $result[1].ac == 'api'
		&& type::is::object($result[1].grant)
		&& $result[1].grant.key = '[REDACTED]'
		&& 'id' in $result[1].grant

		&& $result[2].ac == 'api'
		&& type::is::object($result[2].grant)
		&& $result[2].grant.key = '[REDACTED]'
		&& 'id' in $result[2].grant

		&& $result[3].ac == 'api'
		&& type::is::object($result[3].grant)
		&& $result[3].grant.key = '[REDACTED]'
		&& 'id' in $result[3].grant

'''


[[test.results]]
match = '''
	array::len($result) == 4

		&& $result[0].ac == 'api'
		&& type::is::object($result[0].grant)
		&& $result[0].grant.key = '[REDACTED]'
		&& 'id' in $result[0].grant

		&& $result[1].ac == 'api'
		&& type::is::object($result[1].grant)
		&& $result[1].grant.key = '[REDACTED]'
		&& 'id' in $result[1].grant

		&& $result[2].ac == 'api'
		&& type::is::object($result[2].grant)
		&& $result[2].grant.key = '[REDACTED]'
		&& 'id' in $result[2].grant

		&& $result[3].ac == 'api'
		&& type::is::object($result[3].grant)
		&& $result[3].grant.key = '[REDACTED]'
		&& 'id' in $result[3].grant

'''

[[test.results]]
error = "The access method 'invalid' does not exist in the namespace 'test'"

[[test.results]]
error = "The access method 'invalid' does not exist in the namespace 'test'"

[[test.results]]
error = "The user 'invalid' does not exist in the namespace 'test'"

[[test.results]]
error = "The user 'invalid' does not exist in the namespace 'test'"

[[test.results]]
error = "The access method 'invalid' does not exist in the namespace 'test'"

[[test.results]]
error = "The access method 'invalid' does not exist in the namespace 'test'"
*/


-- Initial setup
DEFINE ACCESS api ON NAMESPACE TYPE BEARER FOR USER;
DEFINE USER tobie ON NAMESPACE PASSWORD 'secret' ROLES EDITOR;
INFO FOR NAMESPACE;
-- Should succeed
ACCESS api ON NAMESPACE GRANT FOR USER tobie;
ACCESS api ON NAMESPACE GRANT FOR USER tobie;
ACCESS api GRANT FOR USER tobie;
ACCESS api GRANT FOR USER tobie;
ACCESS api ON NAMESPACE SHOW ALL;
ACCESS api SHOW ALL;
-- Should fail
ACCESS invalid ON NAMESPACE GRANT FOR USER tobie;
ACCESS invalid GRANT FOR USER tobie;
ACCESS api ON NAMESPACE GRANT FOR USER invalid;
ACCESS api GRANT FOR USER invalid;
ACCESS invalid ON NAMESPACE SHOW ALL;
ACCESS invalid SHOW ALL;
