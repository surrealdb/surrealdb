/**
[env]
namespace = true
database = true
imports = ["access/record/diff_output_field_permissions_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
reason = """
`RETURN DIFF` describes the mutation to a caller whose field permissions the
two diffed views already applied, so an op appears for exactly the fields that
caller can select and carries the value it may read. A patch-op list is not a
record image, so the name-keyed permission pass on the output must not run over
it: the op objects are keyed `op` / `path` / `value`, and matching a field name
against those cut whole ops out — or, for a table that defines a field named
`value`, the payload of every op, leaving one that reads as "replace with
nothing".
"""

[[test.results]]
value = "user:alice"

[[test.results]]
value = "[[{ op: 'replace', path: '/counter', value: 7 }, { op: 'replace', path: '/shown', value: 21 }]]"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[[{ op: 'replace', path: '/counter', value: 8 }, { op: 'replace', path: '/shown', value: 24 }]]"

[[test.results]]
value = "[{ counter: 8 }]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { counter: 2, id: t:2, secret_set: false, shown: 6 } }]]"
*/

RETURN $auth;

-- A visible stored field, and the visible computed field derived from it, each
-- get an op carrying the new value. The denied computed field derived from the
-- same write gets none.
UPDATE t:1 SET counter = 7 RETURN DIFF;

-- Writing only fields the caller cannot select produces no ops at all, rather
-- than one naming a denied field. `secret_set` does not move either: a computed
-- field is derived from the reduced record, where `secret` is already absent,
-- so a readable computed field cannot report a denied one either.
UPDATE t:1 SET secret = 'STORED_SECRET' RETURN DIFF;

-- A field named after an op key costs no other op its payload: `/counter` still
-- carries 8, and `value` itself stays undescribed.
UPDATE t:1 SET value = 'PAYLOAD_SECRET', counter = 8 RETURN DIFF;

-- What the caller reads back afterwards.
SELECT counter FROM t:1;

-- A creating diff adds the visible fields only, `secret_set` among them reading
-- false for the same reason.
CREATE t:2 SET counter = 2, secret = 'x' RETURN DIFF;
