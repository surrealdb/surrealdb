/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Indexed definitions inside a defined array `id` (`id[0]`, `id[1]`) hold the record-id key to their types, as a tuple kind on `id` does"
issue = 5395

[[test.results]]
value = "NONE"

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
value = "[{ id: test:['aaa', accounts:0] }]"

[[test.results]]
error = "Couldn't coerce value for field `id[0]` of `test:[1, accounts:0]`: Expected `string` but found `1`"

[[test.results]]
error = "Couldn't coerce value for field `id[1]` of `test:['bbb', 'accounts:0']`: Expected `record<accounts>` but found `'accounts:0'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test2:['aaa', accounts:0] }]"

[[test.results]]
error = "Couldn't coerce value for field `id` of `test2:[1, accounts:0]`: Expected `[string, record<accounts>]` but found `[1, accounts:0]`"
*/

DEFINE TABLE accounts SCHEMALESS;
DEFINE TABLE test SCHEMALESS;
DEFINE TABLE test2 SCHEMALESS;

CREATE accounts:0;

DEFINE FIELD id ON test TYPE array;
DEFINE FIELD id[0] ON test TYPE string;
DEFINE FIELD id[1] ON test TYPE record<accounts>;

CREATE test:['aaa', accounts:0] CONTENT {
	id: ['aaa', accounts:0]
};
CREATE test:[1, accounts:0];
CREATE test:['bbb', 'accounts:0'];

DEFINE FIELD id ON test2 TYPE [string, record<accounts>];

CREATE test2:['aaa', accounts:0] CONTENT {
	id: ['aaa', accounts:0]
};
CREATE test2:[1, accounts:0];
