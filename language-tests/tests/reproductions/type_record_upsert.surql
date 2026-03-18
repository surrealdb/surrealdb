/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "UPSERT ONLY with type::record() to construct a record ID dynamically"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ id: flow:test_flow, status: 'active', version: 1 }"

[[test.results]]
value = "[{ id: flow:test_flow, status: 'active', version: 1 }]"
*/

LET $name = "test_flow";
LET $obj = { status: "active", version: 1 };

UPSERT ONLY type::record('flow', $name) CONTENT $obj;

-- Verify the record was created with the correct ID and content
SELECT * FROM flow:test_flow;
