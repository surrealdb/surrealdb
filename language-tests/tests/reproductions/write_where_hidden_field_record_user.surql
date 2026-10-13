/**
[env]
namespace = true
database = true
imports = ["reproductions/write_where_hidden_field_record_user_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
reason = "A record user's UPDATE or DELETE judges its WHERE against the record as the user may select it, where a field the user may write but not select is NONE, with or without an index over the field: `hidden = 'x'` matches no row, `hidden != 'x'` and `hidden IS NONE` match every row, and a conjunct beside it is judged as written, so which rows a write touches reveals nothing of the field's value. A record the table's SELECT permission hides is matched by no WHERE and returned by no write."

[[test.results]]
value = "user:alice"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: plain:1 }, { id: plain:2 }, { id: plain:3 }]"

[[test.results]]
value = "[{ id: plain:1 }, { id: plain:2 }, { id: plain:3 }]"

[[test.results]]
value = "[{ id: plain:1 }, { id: plain:2 }]"

[[test.results]]
value = "[{ a: NONE, b: true, c: true, d: true, id: plain:1 }, { a: NONE, b: true, c: true, d: true, id: plain:2 }, { a: NONE, b: true, c: true, d: NONE, id: plain:3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: indexed:1 }, { id: indexed:2 }, { id: indexed:3 }]"

[[test.results]]
value = "[{ id: indexed:1 }, { id: indexed:2 }, { id: indexed:3 }]"

[[test.results]]
value = "[{ id: indexed:1 }, { id: indexed:2 }]"

[[test.results]]
value = "[{ a: NONE, b: true, c: true, d: true, id: indexed:1 }, { a: NONE, b: true, c: true, d: true, id: indexed:2 }, { a: NONE, b: true, c: true, d: NONE, id: indexed:3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: gone_eq:1 }, { id: gone_eq:2 }, { id: gone_eq:3 }]"

[[test.results]]
value = "[{ id: gone_ne:1, shown: 1 }, { id: gone_ne:2, shown: 1 }, { id: gone_ne:3, shown: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: gone_eq_indexed:1 }, { id: gone_eq_indexed:2 }, { id: gone_eq_indexed:3 }]"

[[test.results]]
value = "[{ id: gone_ne_indexed:1, shown: 1 }, { id: gone_ne_indexed:2, shown: 1 }, { id: gone_ne_indexed:3, shown: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: veiled:2 }]"

[[test.results]]
value = "[{ id: veiled:2 }]"

[[test.results]]
value = "[{ f: true, g: true, hidden: 'x', id: veiled:2, shown: 2 }]"

*/

RETURN $auth;

UPDATE plain SET a = true WHERE hidden = 'x' RETURN id;
UPDATE plain SET b = true WHERE hidden != 'x' RETURN id;
UPDATE plain SET c = true WHERE hidden IS NONE RETURN id;
UPDATE plain SET d = true WHERE shown = 1 AND hidden != 'x' RETURN id;
SELECT id, a, b, c, d FROM plain ORDER BY id;

UPDATE indexed SET a = true WHERE hidden = 'x' RETURN id;
UPDATE indexed SET b = true WHERE hidden != 'x' RETURN id;
UPDATE indexed SET c = true WHERE hidden IS NONE RETURN id;
UPDATE indexed SET d = true WHERE shown = 1 AND hidden != 'x' RETURN id;
SELECT id, a, b, c, d FROM indexed ORDER BY id;

DELETE gone_eq WHERE hidden = 'x' RETURN BEFORE;
SELECT id FROM gone_eq ORDER BY id;

DELETE gone_ne WHERE hidden != 'x' RETURN BEFORE;
SELECT id FROM gone_ne ORDER BY id;

DELETE gone_eq_indexed WHERE hidden = 'x' RETURN BEFORE;
SELECT id FROM gone_eq_indexed ORDER BY id;

DELETE gone_ne_indexed WHERE hidden != 'x' RETURN BEFORE;
SELECT id FROM gone_ne_indexed ORDER BY id;

UPDATE veiled SET e = true WHERE shown = 1 RETURN id;
UPDATE veiled SET f = true WHERE hidden = 'x' RETURN id;
UPDATE veiled SET g = true RETURN id;
SELECT * FROM veiled ORDER BY id;
