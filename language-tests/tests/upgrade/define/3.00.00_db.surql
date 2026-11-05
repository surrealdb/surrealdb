/**
[env]
remote-exclusive = true
imports = ["upgrade/define/3.00.00_db_import.surql"]
clean = true

[test]
upgrade = true

[[test.results]]
value = "{ database: 'test', namespace: 'test_namespace' }"

# A released binary reports no compatibility level and no namespace buckets;
# the current build reads the database a released binary defined at level 3,
# and defines it at its default of 4 when it replays the import itself outside
# an upgrade.
[[test.results]]
match = '''$result IN [
	{ accesses: {}, databases: { test_database: "DEFINE DATABASE test_database COMMENT 'a test database'" }, users: {} },
	{ accesses: {}, databases: { test_database: "DEFINE DATABASE test_database COMPATIBILITY 3 COMMENT 'a test database'" }, users: {} },
	{ accesses: {}, databases: { test_database: "DEFINE DATABASE test_database COMPATIBILITY 4 COMMENT 'a test database'" }, users: {} },
	{ accesses: {}, buckets: {}, databases: { test_database: "DEFINE DATABASE test_database COMPATIBILITY 3 COMMENT 'a test database'" }, users: {} },
	{ accesses: {}, buckets: {}, databases: { test_database: "DEFINE DATABASE test_database COMPATIBILITY 4 COMMENT 'a test database'" }, users: {} },
]'''


*/
USE NS test_namespace;
INFO FOR NAMESPACE;
