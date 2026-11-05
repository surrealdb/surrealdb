/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Path definitions inside a defined object `id` (`id.id`, `id.parent`) hold the record-id key to their types"
issue = 5395

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: accounts:0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:{ id: 'aaa', parent: accounts:0 } }]"

[[test.results]]
value = "[{ id: test:{ id: 'bbb', parent: accounts:0 } }]"

[[test.results]]
value = "[{ id: test:{ id: 'aaa', parent: accounts:0 }, name: 'updated' }]"

[[test.results]]
value = "[{ id: test:{ id: 'aaa', parent: accounts:0 }, name: 'updated' }, { id: test:{ id: 'bbb', parent: accounts:0 } }]"

[[test.results]]
error = "Couldn't coerce value for field `id.id` of `test:{ id: 1, parent: accounts:0 }`: Expected `string` but found `1`"

[[test.results]]
error = "Couldn't coerce value for field `id.parent` of `test:{ id: 'ccc' }`: Expected `record<accounts>` but found `NONE`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `id.parent` of `test2:{ id: 'aaa', parent: accounts:0 }`: Expected `string` but found `accounts:0`"

[[test.results]]
value = "[]"
*/

DEFINE TABLE accounts SCHEMALESS;
DEFINE TABLE test SCHEMAFULL;

CREATE accounts:0;

DEFINE FIELD id ON test TYPE object;
DEFINE FIELD id.id ON test TYPE string;
DEFINE FIELD id.parent ON test TYPE record<accounts>;
DEFINE FIELD name ON test TYPE option<string>;

CREATE test:{ id: 'aaa', parent: accounts:0 };
CREATE test:{ id: 'bbb', parent: accounts:0 } CONTENT {
	id: { id: 'bbb', parent: accounts:0 },
};
UPDATE test:{ id: 'aaa', parent: accounts:0 } SET name = 'updated';
SELECT * FROM test;

-- The wrong type at a defined path
CREATE test:{ id: 1, parent: accounts:0 };
-- A defined path the key does not hold
CREATE test:{ id: 'ccc' };

DEFINE TABLE test2 SCHEMAFULL;
DEFINE FIELD id ON test2 TYPE object;
DEFINE FIELD id.id ON test2 TYPE string;
DEFINE FIELD id.parent ON test2 TYPE string;

CREATE test2:{ id: 'aaa', parent: accounts:0 };
SELECT * FROM test2;
