/**
[env]
namespace = true
database = true

[test]
reason = "DEFINE TYPE OVERWRITE judges the stored values of a field defined under a wildcard whose parent names no type: each field of an object map, a field of each element of an array of objects, and each element of an untyped array"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, map: { a: 1, b: 5 } }]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'posint': the field 'map.*' on the table 't' holds a value it does not admit: Expected `posint (ASSERT $value < 3 failed)` but found `5`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:2, items: [{ n: 1 }, { n: 6 }] }]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'posint': the field 'items.*.n' on the table 't' holds a value it does not admit: Expected `posint (ASSERT $value < 3 failed)` but found `6`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:3, xs: [2, 7] }]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'posint': the field 'xs.*' on the table 't' holds a value it does not admit: Expected `posint (ASSERT $value < 3 failed)` but found `7`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:4, items: [{ n: 2 }], map: { a: 1 }, xs: [1, 2] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:4, items: [{ n: 2 }], map: { a: 1 }, xs: [1, 2] }]"

*/
DEFINE TYPE posint AS int ASSERT $value > 0;
DEFINE FIELD map ON t TYPE option<object>;
DEFINE FIELD map.* ON t TYPE posint;
DEFINE FIELD items ON t TYPE option<array<object>>;
DEFINE FIELD items[*].n ON t TYPE posint;
DEFINE FIELD xs ON t TYPE option<array>;
DEFINE FIELD OVERWRITE xs[*] ON t TYPE posint;
CREATE t:1 SET map = { a: 1, b: 5 };
DEFINE TYPE OVERWRITE posint AS int ASSERT $value < 3;
DELETE t:1;
CREATE t:2 SET items = [{ n: 1 }, { n: 6 }];
DEFINE TYPE OVERWRITE posint AS int ASSERT $value < 3;
DELETE t:2;
CREATE t:3 SET xs = [2, 7];
DEFINE TYPE OVERWRITE posint AS int ASSERT $value < 3;
DELETE t:3;
CREATE t:4 SET map = { a: 1 }, items = [{ n: 2 }], xs = [1, 2];
DEFINE TYPE OVERWRITE posint AS int ASSERT $value < 3;
SELECT * FROM t;
