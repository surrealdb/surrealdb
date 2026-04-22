/**
[env]
timeout = 3000
auth = { level = "owner" }

[test]
reason = "Test ALTER USER with all variations"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''$result.users.testuser = /DEFINE USER testuser ON DATABASE PASSHASH '\$argon2id\$.*' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE/'''
error = false

[[test.results]]
value = "NONE"

[[test.results]]
match = '''$result.users.testuser = /DEFINE USER testuser ON DATABASE PASSHASH '\$argon2id\$.*' ROLES EDITOR DURATION FOR TOKEN 1h, FOR SESSION 1d COMMENT 'updated user'/'''
error = false

[[test.results]]
value = "NONE"

[[test.results]]
match = '''$result.users.testuser = /DEFINE USER testuser ON DATABASE PASSHASH '\$argon2id\$.*' ROLES EDITOR DURATION FOR TOKEN NONE, FOR SESSION NONE/'''
error = false

[[test.results]]
value = "NONE"

[[test.results]]
error = "The user 'nonexistent' does not exist in the database 'test'"

*/

-- Setup
DEFINE USER testuser ON DATABASE PASSWORD 'oldpass' ROLES VIEWER;
INFO FOR DB;

-- Alter: change password, roles, durations, comment
ALTER USER testuser ON DATABASE
	PASSWORD 'newpass'
	ROLES EDITOR
	DURATION FOR TOKEN 1h, FOR SESSION 24h
	COMMENT "updated user";
INFO FOR DB;

-- Alter: drop durations and comment
ALTER USER testuser ON DATABASE
	DURATION FOR TOKEN NONE, FOR SESSION NONE
	DROP COMMENT;
INFO FOR DB;

-- IF EXISTS on non-existent user
ALTER USER IF EXISTS nonexistent ON DATABASE COMMENT "nope";

-- Non-existent user errors
ALTER USER nonexistent ON DATABASE COMMENT "nope";
