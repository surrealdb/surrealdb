/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A LET of `$value` or `$input` is refused: they are names the system binds for an event or a field, so no statement binds one. A statement that reads the name afterwards reads it unbound, so it matches no row."

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
error = "'value' is a protected variable and cannot be set"

[[test.results]]
value = "[]"

[[test.results]]
error = "'input' is a protected variable and cannot be set"

[[test.results]]
value = "[]"
*/

DEFINE TABLE items SCHEMAFULL;
DEFINE FIELD x ON items TYPE int;
CREATE items:1 SET x = 10;
CREATE items:2 SET x = 20;
CREATE items:3 SET x = 30;

-- $value and $input are names the system binds for events and fields, so
-- a LET of either is refused.
LET $value = 20;
SELECT * FROM items WHERE x = $value;

LET $input = 10;
SELECT * FROM items WHERE x = $input;
