/**
[env]
namespace = true
database = true

[test]
reason = """
	Two identical writes of a table run the stages its definitions state when
	each runs: a DEFINE FIELD, DEFINE EVENT, DEFINE INDEX or DEFINE TABLE between
	them changes what the second write coerces, derives, fires, indexes and
	enforces. That holds across statements, across transactions, and inside one
	BEGIN ... COMMIT, whose later writes are planned before the definition ahead
	of them has run.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1.5f, id: ft:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `a` of `ft:1`: Expected `int` but found `1.5f`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ b: 1, id: fv:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ b: 11, id: fv:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ b: 1, c: NONE }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ b: 1, c: 'filled' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 3, id: fa:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found 3 for field `a`, with record `fa:1`, but field must conform to: $value > 5"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 3, id: fa:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: e:1, n: 0 }]"

[[test.results]]
value = "[{ id: e:1, n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: e:1, n: 2 }]"

[[test.results]]
value = "[{ id: e:1, n: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: e:1, n: 4 }]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:1, k: 'same' }]"

[[test.results]]
value = "[{ id: u:2, k: 'same' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Database index `uk` already contains 'same', with record `u:1`"

[[test.results]]
value = "[{ id: u:1, k: 'same' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, extra: true, id: s:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found field 'extra', but no such field exists for table 's'"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1.5f, id: x:1 }]"

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
error = "Couldn't coerce value for field `a` of `x:1`: Expected `int` but found `1.5f`"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: x:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: x:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: y:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 'derived', id: y:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 'derived', id: y:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 'derived', id: y:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1.5f, id: z:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Couldn't coerce value for field `a` of `z:1`: Expected `int` but found `1.5f`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1.5f, id: z:1 }]"

[[test.results]]
value = "[{ a: 1.5f, id: z:1 }]"
*/

-- A field's TYPE, VALUE, DEFAULT and ASSERT, each defined between two identical writes, and an
-- ASSERT removed between two more.
DEFINE TABLE ft SCHEMALESS;
UPSERT ft:1 SET a = 1.5;
DEFINE FIELD a ON ft TYPE int;
UPSERT ft:1 SET a = 1.5;
DEFINE TABLE fv SCHEMALESS;
UPSERT fv:1 SET b = 1;
DEFINE FIELD b ON fv VALUE $value + 10;
UPSERT fv:1 SET b = 1;
DEFINE TABLE fd SCHEMALESS;
CREATE fd SET b = 1 RETURN b, c;
DEFINE FIELD c ON fd TYPE string DEFAULT 'filled';
CREATE fd SET b = 1 RETURN b, c;
DEFINE TABLE fa SCHEMALESS;
UPSERT fa:1 SET a = 3;
DEFINE FIELD a ON fa ASSERT $value > 5;
UPSERT fa:1 SET a = 3;
REMOVE FIELD a ON fa;
UPSERT fa:1 SET a = 3;

-- An event defined between two identical writes, then removed between two more.
DEFINE TABLE e SCHEMALESS;
DEFINE TABLE log SCHEMALESS;
CREATE e:1 SET n = 0;
UPDATE e:1 SET n += 1;
DEFINE EVENT seen ON e WHEN $event = 'UPDATE' THEN { CREATE log SET of = $after.id, n = $after.n };
UPDATE e:1 SET n += 1;
UPDATE e:1 SET n += 1;
REMOVE EVENT seen ON e;
UPDATE e:1 SET n += 1;
SELECT VALUE n FROM log ORDER BY n;

-- A unique index defined between two identical writes.
DEFINE TABLE u SCHEMALESS;
CREATE u:1 SET k = 'same';
UPSERT u:2 SET k = 'same';
DELETE u:2;
DEFINE INDEX uk ON u FIELDS k UNIQUE;
UPSERT u:2 SET k = 'same';
SELECT * FROM u WHERE k = 'same';

-- The table made SCHEMAFULL between two identical writes.
DEFINE TABLE s SCHEMALESS;
UPSERT s:1 SET a = 1, extra = true;
DELETE s;
DEFINE TABLE OVERWRITE s SCHEMAFULL;
DEFINE FIELD a ON s TYPE int;
UPSERT s:1 SET a = 1, extra = true;
SELECT * FROM s;

-- The same changes, each in a transaction of its own.
DEFINE TABLE x SCHEMALESS;
BEGIN;
UPSERT x:1 SET a = 1.5;
COMMIT;
BEGIN;
DEFINE FIELD a ON x TYPE int;
COMMIT;
BEGIN;
UPSERT x:1 SET a = 1.5;
COMMIT;
BEGIN;
DEFINE EVENT bump ON x THEN { UPSERT log:x SET n += 1 };
COMMIT;
BEGIN;
UPSERT x:1 SET a = 1;
COMMIT;
BEGIN;
UPSERT x:1 SET a = 1;
COMMIT;
SELECT n FROM log:x;

-- Inside one transaction, whose writes after a definition are planned before it has run.
DEFINE TABLE y SCHEMALESS;
BEGIN;
UPSERT y:1 SET a = 1;
DEFINE FIELD b ON y VALUE 'derived';
UPSERT y:1 SET a = 1;
DEFINE FIELD a ON y TYPE int ASSERT $value < 5;
UPSERT y:1 SET a = 1;
COMMIT;
SELECT * FROM y;

-- A definition in a cancelled transaction leaves the next write with the definitions it had.
DEFINE TABLE z SCHEMALESS;
UPSERT z:1 SET a = 1.5;
BEGIN;
DEFINE FIELD a ON z TYPE int;
UPSERT z:1 SET a = 1.5;
CANCEL;
UPSERT z:1 SET a = 1.5;
SELECT * FROM z;
