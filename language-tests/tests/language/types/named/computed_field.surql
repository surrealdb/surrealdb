/**
[env]
namespace = true
database = true

[test]
reason = "A computed field of a named type coerces its value to the type and runs the type's checks when it is read, refusing a value the type does not admit"

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
value = "[]"

[[test.results]]
value = "[{ id: t:1, m: 'ok', ok: 5, opt: NONE }]"

[[test.results]]
value = "[{ m: 'ok', ok: 5, opt: NONE }]"

[[test.results]]
value = "[5]"

[[test.results]]
value = "5"

[[test.results]]
value = "{ id: t:1, m: 'ok', ok: 5, opt: NONE }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Couldn't coerce value for field `c` of `u:1`: Expected `capped (ASSERT $value < 10 failed)` but found `20`"

[[test.results]]
error = "Couldn't coerce value for field `c` of `u:1`: Expected `capped (ASSERT $value < 10 failed)` but found `20`"

[[test.results]]
error = "Couldn't coerce value for field `c` of `u:1`: Expected `capped (ASSERT $value < 10 failed)` but found `20`"

[[test.results]]
error = "Failed to coerce computed field 'c'"

[[test.results]]
error = "Failed to coerce computed field 'c'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Couldn't coerce value for field `m` of `v:1`: Expected `mood ('sad' | 'ok')` but found `'meh'`"

[[test.results]]
error = "Couldn't coerce value for field `m` of `v:1`: Expected `mood ('sad' | 'ok')` but found `'meh'`"

[[test.results]]
error = "Failed to coerce computed field 'm'"

[[test.results]]
value = "[]"

[[test.results]]
error = "Failed to coerce computed field 'c'"

[[test.results]]
error = "Failed to coerce computed field 'c'"

*/

DEFINE TYPE capped AS int ASSERT $value < 10;
DEFINE TYPE mood AS 'sad' | 'ok';
-- Values every type admits.
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD ok ON t TYPE capped COMPUTED 5;
DEFINE FIELD opt ON t TYPE option<capped> COMPUTED NONE;
DEFINE FIELD m ON t TYPE mood COMPUTED 'ok';
CREATE t:1 RETURN NONE;
SELECT * FROM t;
SELECT ok, opt, m FROM t;
SELECT VALUE ok FROM t;
RETURN t:1.ok;
RETURN t:1.*;
-- A value the type's ASSERT refuses.
DEFINE TABLE u SCHEMAFULL;
DEFINE FIELD c ON u TYPE capped COMPUTED 20;
CREATE u:1 RETURN NONE;
SELECT * FROM u;
SELECT c FROM u;
SELECT VALUE c FROM u;
RETURN u:1.c;
RETURN u:1.*;
-- A label the union does not hold.
DEFINE TABLE v SCHEMAFULL;
DEFINE FIELD m ON v TYPE mood COMPUTED 'meh';
CREATE v:1 RETURN NONE;
SELECT * FROM v;
SELECT m FROM v;
RETURN v:1.m;
-- A record link dereferenced to a refused computed field.
CREATE w:1 SET link = u:1 RETURN NONE;
SELECT link.c FROM w;
SELECT * FROM w FETCH link;
