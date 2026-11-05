/**
[env]
compatibility = 3
imports = ["upgrade/define/3.00.00_user_duration_import.surql"]

[test]
upgrade = true

# A released binary reports no compatibility level; the current build reads
# the database a released binary defined at level 3.
[[test.results]]
match = '''$result.compatibility IN [NONE, 3]
	&& $result.users == { user_session_duration: "DEFINE USER user_session_duration ON DATABASE PASSHASH '' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION 1h", user_session_none: "DEFINE USER user_session_none ON DATABASE PASSHASH '' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE", user_token_duration: "DEFINE USER user_token_duration ON DATABASE PASSHASH '' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE" }'''
*/
INFO FOR DB;
