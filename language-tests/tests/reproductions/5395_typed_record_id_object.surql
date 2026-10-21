/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A record id whose key is an object of typed parts is declared by one
definition of `id`. Defining `id.id` and `id.parent` separately, as the report
did, is refused until `id` itself is defined, and a CREATE whose id matches the
declared kind succeeds by key or by CONTENT.
"""
issue = 5395

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `id.id`: the field `id` it is nested in is not defined. Define `id` first"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:{ id: 'aaa', parent: accounts:0 } }]"

[[test.results]]
value = "[{ id: test:{ id: 'bbb', parent: accounts:0 } }]"

[[test.results]]
error = "Couldn't coerce value for field `id` of `test:{ id: 'ccc', parent: 'x' }`: Expected `{ id: string, parent: record<accounts> }` but found `{ id: 'ccc', parent: 'x' }`"
*/
DEFINE TABLE test SCHEMAFUL;
DEFINE FIELD id.id ON test TYPE string;
DEFINE TABLE accounts;
DEFINE FIELD id ON test TYPE { id: string, parent: record<accounts> };
CREATE test:{ id: 'aaa', parent: accounts:000 } CONTENT { id: { id: 'aaa', parent: accounts:000 } };
CREATE test CONTENT { id: { id: 'bbb', parent: accounts:000 } };
CREATE test:{ id: 'ccc', parent: 'x' };
