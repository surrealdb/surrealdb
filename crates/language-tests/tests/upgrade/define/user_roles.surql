/**
[env]
imports = ["upgrade/define/user_roles_import.surql"]

[test]
upgrade = true

[[test.results]]
match = '''$result.users == { root_editor: "DEFINE USER root_editor ON DATABASE PASSHASH '' ROLES EDITOR DURATION FOR TOKEN 1h, FOR SESSION NONE", root_owner: "DEFINE USER root_owner ON DATABASE PASSHASH '' ROLES OWNER DURATION FOR TOKEN 1h, FOR SESSION NONE", root_viewer: "DEFINE USER root_viewer ON DATABASE PASSHASH '' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE" }'''

*/
INFO FOR DB;
