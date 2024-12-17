/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
match = """
$result.users.test = /DEFINE USER test ON ROOT PASSHASH '\\$argon2id\\$.*' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE/
"""
error = false
*/

DEFINE USER test ON ROOT PASSWORD 'test';
INFO FOR ROOT;
