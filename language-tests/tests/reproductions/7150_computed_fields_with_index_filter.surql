/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "WHERE filters on indexed fields must include COMPUTED fields in results"
issue = 7150

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ computed_val: 'TEST1', id: test:1, name: 'test1', testval: 'val1' }]"

[[test.results]]
value = "[{ computed_val: 'TEST1' }]"

[[test.results]]
value = "[{ computed_val: 'TEST1' }]"

[[test.results]]
value = "[{ computed_val: 'TEST1', id: test:1, name: 'test1', testval: 'val1' }, { computed_val: 'TEST2', id: test:2, name: 'test2', testval: 'val2' }]"
*/

-- Setup: table with a regular field, an indexed field, and a computed field
{
	DEFINE TABLE test;
	DEFINE FIELD name ON test TYPE string;
	DEFINE FIELD testval ON test TYPE string;
	DEFINE FIELD computed_val ON test COMPUTED string::uppercase(name);

	DEFINE INDEX name_idx ON test FIELDS name;

	INSERT INTO test [
		{ id: test:1, name: "test1", testval: "val1" },
		{ id: test:2, name: "test2", testval: "val2" },
	];

	RETURN "OK";
};

-- Indexed filter: SELECT * should include computed field
SELECT * FROM test WHERE name = 'test1';

-- Indexed filter: SELECT computed field explicitly should not be NONE
SELECT computed_val FROM test WHERE name = 'test1';

-- Non-indexed filter (table scan): computed field should work (baseline)
SELECT computed_val FROM test WHERE testval = 'val1';

-- No filter: all computed fields should be present (baseline)
SELECT * FROM test;
