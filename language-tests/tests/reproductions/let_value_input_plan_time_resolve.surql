/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "LET $value and $input bindings must be resolvable at plan time despite sharing names with doc-scoped params"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: items:1, x: 10 }]"

[[test.results]]
value = "[{ id: items:2, x: 20 }]"

[[test.results]]
value = "[{ id: items:3, x: 30 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: items:2, x: 20 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: items:1, x: 10 }]"
*/

DEFINE TABLE items SCHEMAFULL;
DEFINE FIELD x ON items TYPE int;
CREATE items:1 SET x = 10;
CREATE items:2 SET x = 20;
CREATE items:3 SET x = 30;

-- $value and $input share names with doc-scoped event variables, but
-- when set via LET they must be resolved normally at plan time.
LET $value = 20;
SELECT * FROM items WHERE x = $value;

LET $input = 10;
SELECT * FROM items WHERE x = $input;
