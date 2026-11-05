/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "An ASSERT on a definition inside `id` is judged against the record-id key, and a VALUE or DEFAULT there is not applied, since the key is fixed once settled"
issue = 5395

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
value = "[{ id: test:{ id: 'allowed', n: 1 } }]"

[[test.results]]
error = "Found 'blocked' for field `id.id`, with record `test:{ id: 'blocked', n: 1 }`, but field must conform to: $value = 'allowed'"

[[test.results]]
value = "[{ id: test:{ id: 'allowed', n: 1 } }]"
*/

DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD id ON test TYPE object;
DEFINE FIELD id.id ON test TYPE string ASSERT $value = 'allowed';
DEFINE FIELD id.n ON test TYPE int VALUE $value + 1;
DEFINE FIELD id.d ON test TYPE option<int> DEFAULT 5;

CREATE test:{ id: 'allowed', n: 1 };
CREATE test:{ id: 'blocked', n: 1 };
SELECT * FROM test;
