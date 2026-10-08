/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A delete of several records unsets each from the collection that references it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: root:1 }, { id: root:2 }, { id: root:3 }]"

[[test.results]]
value = "[{ id: side:1, many: [root:1, root:2, root:1, root:3], one: root:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: side:1, many: [root:3] }]"
*/

DEFINE TABLE root;
DEFINE TABLE side;
DEFINE FIELD many ON side TYPE option<array<record<root>>> REFERENCE ON DELETE UNSET;
DEFINE FIELD one ON side TYPE option<record<root>> REFERENCE ON DELETE UNSET;
CREATE root:1, root:2, root:3;
CREATE side:1 SET many = [root:1, root:2, root:1, root:3], one = root:2;
DELETE root:1, root:2;
SELECT * FROM side;
