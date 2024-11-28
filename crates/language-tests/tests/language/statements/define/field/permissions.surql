/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = """
{
	events: {},
	fields: { test: "DEFINE FIELD test ON user PERMISSIONS FOR select, create, update WHERE true" },
	tables: {},
	indexes: {},
	lives: {},
}"""

*/
DEFINE FIELD test ON user PERMISSIONS FOR select, create, update, delete WHERE true;
INFO FOR TABLE user;
