/**
[env]
imports = ["upgrade/define/user_duration_import.surql"]

[test]
upgrade = true

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: { user_session_duration: "DEFINE USER user_session_duration ON DATABASE PASSHASH '' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION 1h", user_session_none: "DEFINE USER user_session_none ON DATABASE PASSHASH '' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE", user_token_duration: "DEFINE USER user_token_duration ON DATABASE PASSHASH '' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE" } }'''
*/
INFO FOR DB;
