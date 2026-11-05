/**
[env]
namespace = true
database = true

[test]
reason = "DEFINE TYPE OVERWRITE refuses a stored NULL, object or array a replacement's check fails with the check's refusal, rendering the value found as SurrealQL"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, v: NULL }]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'anything': the field 'v' on the table 't' holds a value it does not admit: Expected `anything (ASSERT $value != NULL failed)` but found `NULL`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:2, v: { a: 1 } }]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'anything': the field 'v' on the table 't' holds a value it does not admit: Expected `anything (ASSERT false failed)` but found `{ a: 1 }`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:3, v: [1, 'x'] }]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'anything': the field 'v' on the table 't' holds a value it does not admit: Expected `anything (ASSERT false failed)` but found `[1, 'x']`"

*/
DEFINE TYPE anything AS any;
DEFINE FIELD v ON t TYPE anything;
CREATE t:1 SET v = NULL;
DEFINE TYPE OVERWRITE anything AS any ASSERT $value != NULL;
DELETE t:1;
CREATE t:2 SET v = { a: 1 };
DEFINE TYPE OVERWRITE anything AS any ASSERT false;
DELETE t:2;
CREATE t:3 SET v = [1, 'x'];
DEFINE TYPE OVERWRITE anything AS any ASSERT false;
