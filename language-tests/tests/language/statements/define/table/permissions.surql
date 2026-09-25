/**
[test]
reason = "A table's PERMISSIONS clause reports every action it grants: an action left out of the list reparses as NONE, so a dropped clause revokes it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

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
	modules: {},
	params: {},
	sequences: {},
	tables: {
			default: 'DEFINE TABLE default TYPE ANY SCHEMALESS PERMISSIONS NONE',
			delete_only: 'DEFINE TABLE delete_only TYPE ANY SCHEMALESS PERMISSIONS FOR select, create, update NONE, FOR delete FULL',
			full: 'DEFINE TABLE full TYPE ANY SCHEMALESS PERMISSIONS FULL',
			select_delete_full: 'DEFINE TABLE select_delete_full TYPE ANY SCHEMALESS PERMISSIONS FOR select, delete FULL, FOR create, update NONE',
			select_full: 'DEFINE TABLE select_full TYPE ANY SCHEMALESS PERMISSIONS FOR select FULL, FOR create, update, delete NONE'
	},
	users: {}
}"""

[[test.results]]
value = "[{ create: false, delete: true, select: true, update: false }]"
*/

DEFINE TABLE default;
DEFINE TABLE select_full PERMISSIONS FOR select FULL;
DEFINE TABLE full PERMISSIONS FULL;
DEFINE TABLE select_delete_full PERMISSIONS FOR select, delete FULL;
DEFINE TABLE delete_only PERMISSIONS FOR delete FULL;
INFO FOR DB;

-- The rendered definition above has to agree with what is stored: re-applying
-- it must not narrow the grant.
RETURN (INFO FOR DB STRUCTURE).tables[WHERE name = 'select_delete_full'].permissions;
