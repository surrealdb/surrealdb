/**
[env]
compatibility = 3
imports = ["upgrade/define/3.00.00_table_import.surql"]

[test]
upgrade = true

# A released binary reports no compatibility level; the current build reads
# the database a released binary defined at level 3.
[[test.results]]
match = '''$result.compatibility IN [NONE, 3]
	&& $result.tables == { table: "DEFINE TABLE `table` TYPE NORMAL DROP SCHEMAFULL COMMENT 'foo' PERMISSIONS NONE", table_full: "DEFINE TABLE table_full TYPE NORMAL DROP SCHEMAFULL COMMENT 'foo' PERMISSIONS FULL", table_specific: "DEFINE TABLE table_specific TYPE NORMAL DROP SCHEMAFULL COMMENT 'foo' PERMISSIONS FOR select FULL, FOR create WHERE a = 1, FOR update, delete NONE" }'''
*/

INFO FOR DB;
