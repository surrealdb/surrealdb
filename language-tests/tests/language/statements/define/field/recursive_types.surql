/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
{
	events: {},
	fields: {
		"foo": "DEFINE FIELD foo ON bar TYPE array<float | array<int>> | set<number> PERMISSIONS FULL",
		"foo.*": "DEFINE FIELD foo.* ON bar TYPE float | array<int> | number PERMISSIONS FULL",
		"foo.*.*": "DEFINE FIELD foo.*.* ON bar TYPE int PERMISSIONS FOR select, create FULL, FOR update NONE"
	},
	indexes: {},
	lives: {},
	tables: {}
}
'''


*/
DEFINE TABLE bar;
// this recursively defines types for foo.* and foo.*.*
DEFINE FIELD foo ON bar TYPE array<float | array<bool>> | set<number>;
// a field the type implies takes clauses of its own
DEFINE FIELD OVERWRITE foo.*.* ON bar PERMISSIONS FOR UPDATE NONE;
// redefining the parent's type redefines the type of each field it implies,
// and keeps their other clauses
DEFINE FIELD OVERWRITE foo ON bar TYPE array<float | array<int>> | set<number>;
INFO FOR TABLE bar;
