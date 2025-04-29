/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{
	accesses: {},
	analyzers: {},
	apis: {},
	buckets: {},
	configs: {},
	functions: {},
	models: {},
	params: {},
	sequences: {},
	tables: {
			default: 'DEFINE TABLE default TYPE ANY SCHEMALESS PERMISSIONS NONE',
			full: 'DEFINE TABLE full TYPE ANY SCHEMALESS PERMISSIONS FULL',
			select_full: 'DEFINE TABLE select_full TYPE ANY SCHEMALESS PERMISSIONS FOR select FULL, FOR create, update, delete NONE'
	},
	users: {}
}"""
*/

DEFINE TABLE default;
DEFINE TABLE select_full PERMISSIONS FOR select FULL;
DEFINE TABLE full PERMISSIONS FULL;
INFO FOR DB;
