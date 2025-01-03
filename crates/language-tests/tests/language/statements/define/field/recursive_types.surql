/**
[test]

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
		"foo": "DEFINE FIELD foo ON bar TYPE array<float | array<bool>> | set<number> PERMISSIONS FULL",
		"foo[*]": "DEFINE FIELD foo[*] ON bar TYPE float | array<bool> | number PERMISSIONS FULL",
		"foo[*][*]": "DEFINE FIELD foo[*][*] ON bar TYPE bool PERMISSIONS FOR select, create FULL, FOR update NONE"
	},
	indexes: {},
	lives: {},
	tables: {}
}
'''


*/
DEFINE TABLE bar;
// defining a type for the parent type should overwrite permissions for the child.
DEFINE FIELD foo.*.* ON bar TYPE number PERMISSIONS FOR UPDATE NONE;
// this should recursively define types for foo, foo.*, and foo.*.*
DEFINE FIELD foo ON bar TYPE array<float | array<bool>> | set<number>;
INFO FOR TABLE bar;
