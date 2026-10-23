/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A write whose WHERE compares stored fields with literals judges its subjects on their stored bytes before decoding them, and changes exactly the records the WHERE matches: a missing field is NONE, a nested path reads the stored object, a value of another type matches nothing, and a COMPUTED field is judged on the derived value"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, id: t:2, o: { k: 'x' } }]"

[[test.results]]
value = "[{ id: t:3 }]"

[[test.results]]
value = "[{ a: '1', b: true, id: t:4 }]"

[[test.results]]
value = "[{ a: 2, id: t:5, o: { k: 'y' } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 4, d: 8, id: c:1 }]"

[[test.results]]
value = "[true]"

[[test.results]]
value = "[true]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: '1', b: true, id: t:4 }, { a: 2, id: t:5, o: { k: 'y' } }]"

[[test.results]]
value = "1"
*/
INSERT INTO t [
	{ id: t:1, a: 1, o: { k: 'x' } },
	{ id: t:2, a: 1, o: { k: 'x' } },
	{ id: t:3 },
	{ id: t:4, a: '1' },
	{ id: t:5, a: 2, o: { k: 'y' } },
] RETURN NONE;
-- No record holds this value.
DELETE t WHERE a = -1 RETURN BEFORE;
-- A nested path over the stored object, with a conjunction.
DELETE t WHERE o.k = 'x' AND id = t:2 RETURN BEFORE;
-- A missing field reads as NONE.
DELETE t WHERE a = NONE RETURN BEFORE;
-- A string never equals the integer.
UPDATE t SET b = true WHERE a = '1';
-- Every remaining record but the one with a string `a` and the one deleted above.
UPDATE t WHERE a != '1' AND o.k != 'x';
DEFINE FIELD a ON c TYPE int;
DEFINE FIELD d ON c COMPUTED a * 2;
CREATE c:1 SET a = 4 RETURN NONE;
DELETE c WHERE d = 8 RETURN BEFORE;
-- An UPSERT over the table that matches no record creates one.
UPSERT t SET a = 5, made = true WHERE a = 5 RETURN VALUE made;
-- It then matches the record it created.
UPSERT t SET b = false WHERE a = 5 RETURN VALUE made;
DELETE t WHERE a = 1 RETURN NONE;
SELECT * FROM t WHERE made IS NONE ORDER BY id;
count(SELECT * FROM t WHERE made = true AND b = false);
