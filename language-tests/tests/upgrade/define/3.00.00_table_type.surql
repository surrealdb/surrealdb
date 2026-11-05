/**
[env]
compatibility = 3
imports = ["upgrade/define/3.00.00_table_type_import.surql"]

[test]
upgrade = true

# A released binary reports no compatibility level; the current build reads
# the database a released binary defined at level 3.
[[test.results]]
match = '''$result.compatibility IN [NONE, 3]
	&& $result.tables == { any: 'DEFINE TABLE any TYPE ANY SCHEMALESS PERMISSIONS NONE', normal: 'DEFINE TABLE normal TYPE NORMAL SCHEMALESS PERMISSIONS NONE', relation: 'DEFINE TABLE relation TYPE RELATION IN normal OUT any SCHEMALESS PERMISSIONS NONE', relation_enforced: 'DEFINE TABLE relation_enforced TYPE RELATION IN normal OUT any ENFORCED SCHEMALESS PERMISSIONS NONE', relation_none: 'DEFINE TABLE relation_none TYPE RELATION SCHEMALESS PERMISSIONS NONE' }'''

*/
INFO FOR DB;
