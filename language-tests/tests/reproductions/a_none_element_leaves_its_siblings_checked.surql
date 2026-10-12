/**
[env]
namespace = true
database = true

[test]
reason = "An element left NONE under an optional element kind skips only its own nested fields: its siblings' nested fields are still checked"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `obj.*.name` of `item:1`: Expected `string` but found `5`"

[[test.results]]
error = "Couldn't coerce value for field `obj.*.name` of `item:2`: Expected `string` but found `5`"

[[test.results]]
value = "[{ id: item:3, obj: { b: { name: 'x' } } }]"

[[test.results]]
value = "[{ id: item:3, obj: { b: { name: 'x' } } }]"
*/

DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD obj ON item TYPE object;
DEFINE FIELD obj.* ON item TYPE option<object>;
DEFINE FIELD obj.*.name ON item TYPE string;
CREATE item:1 SET obj = { a: NONE, b: { name: 5 } };
CREATE item:2 SET obj = { b: { name: 5 } };
CREATE item:3 SET obj = { a: NONE, b: { name: 'x' } };
SELECT * FROM item;
