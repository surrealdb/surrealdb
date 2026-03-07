/**
[test]
reason = "Test DEFINE USER with DURATION for token and session"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''
	'admin' IN $result.users
		&& 'viewer' IN $result.users
		&& string::contains($result.users.admin, 'DURATION FOR TOKEN 1h, FOR SESSION 1d')
		&& string::contains($result.users.viewer, 'DURATION FOR TOKEN 15m, FOR SESSION 8h')
'''

*/

DEFINE USER admin ON DATABASE PASSWORD 'secret' ROLES OWNER DURATION FOR TOKEN 1h FOR SESSION 24h;
DEFINE USER viewer ON DATABASE PASSWORD 'secret' ROLES VIEWER DURATION FOR TOKEN 15m FOR SESSION 8h;
INFO FOR DATABASE;
