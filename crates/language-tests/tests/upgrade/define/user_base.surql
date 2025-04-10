/**
[env]
imports = ["upgrade/define/user_base_import.surql"]

[test]
upgrade = true

[[test.results]]
match = ''' $result.users.root_user ==  "DEFINE USER root_user ON ROOT PASSHASH '' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE" '''

[[test.results]]
value = '''{ accesses: {  }, databases: { test: 'DEFINE DATABASE test' }, users: { namespace_user: "DEFINE USER namespace_user ON NAMESPACE PASSHASH '' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE" } }'''

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: { database_user: "DEFINE USER database_user ON DATABASE PASSHASH '' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE" } }'''
*/
INFO FOR ROOT;
INFO FOR NAMESPACE;
INFO FOR DATABASE;
