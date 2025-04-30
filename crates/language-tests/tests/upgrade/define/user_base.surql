/**
[env]
imports = ["upgrade/define/user_base_import.surql"]
clean = true

[test]
upgrade = true

[[test.results]]
match = '''$result.users.root_user == "DEFINE USER root_user ON ROOT PASSHASH '' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE"'''

[[test.results]]
match = '''$result.users == { namespace_user: "DEFINE USER namespace_user ON NAMESPACE PASSHASH '' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE" }'''

[[test.results]]
match = '''$result.users == { database_user: "DEFINE USER database_user ON DATABASE PASSHASH '' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE" }'''
*/
INFO FOR ROOT;
INFO FOR NAMESPACE;
INFO FOR DATABASE;
