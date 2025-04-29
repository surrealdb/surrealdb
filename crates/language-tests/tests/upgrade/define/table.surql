/**
[env]
imports = ["upgrade/define/table_import.surql"]

[test]
upgrade = true

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { table: "DEFINE TABLE `table` TYPE NORMAL DROP SCHEMAFULL COMMENT 'foo' PERMISSIONS NONE", table_full: "DEFINE TABLE table_full TYPE NORMAL DROP SCHEMAFULL COMMENT 'foo' PERMISSIONS FULL", table_specific: "DEFINE TABLE table_specific TYPE NORMAL DROP SCHEMAFULL COMMENT 'foo' PERMISSIONS FOR select FULL, FOR create WHERE a = 1, FOR update, delete NONE" }, users: {  } }'''
*/

INFO FOR DB;
