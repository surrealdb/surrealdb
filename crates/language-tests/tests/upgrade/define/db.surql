/**
[env]
imports = ["upgrade/define/db_import.surql"]
clean = true

[test]
upgrade = true

[[test.results]]
value = "{ database: 'test', namespace: 'test_namespace' }"

[[test.results]]
value = '''{ accesses: {  }, databases: { test_database: "DEFINE DATABASE test_database COMMENT 'a test database'" }, users: {  } }'''


*/
USE NS test_namespace;
INFO FOR NAMESPACE;
