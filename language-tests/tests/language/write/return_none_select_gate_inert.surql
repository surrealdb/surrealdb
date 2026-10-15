/**
[env]
imports = ["./return_none_select_gate_inert_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
Under `RETURN NONE` a write shows no record, so the table's select permission
cannot change what the statement answers or stores: it answers `[]` and leaves
what the stages stored, whether the permission admits the written record or not.
This runs every write form as a record user whose select permission admits only
the records it owns, and reads what was stored through views that no permission
hides.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: item_state:1, n: 101, owner: user:alice }, { id: item_state:10, n: 110, owner: user:alice }, { id: item_state:11, n: 111, owner: user:bob }, { id: item_state:12, n: 12, owner: user:alice }, { id: item_state:13, n: 13, owner: user:bob }, { id: item_state:14, n: 16, owner: user:bob }, { id: item_state:15, n: 17, owner: user:alice }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: item:1, n: 101, owner: user:alice }, { id: item:10, n: 110, owner: user:alice }, { id: item:12, n: 12, owner: user:alice }, { id: item:15, n: 17, owner: user:alice }]"
*/

CREATE item:10 SET owner = user:alice, n = 10 RETURN NONE;
CREATE item:11 SET owner = user:bob, n = 11 RETURN NONE;
UPDATE item SET n = n + 100 RETURN NONE;
UPDATE item:2 SET n = 7 RETURN NONE;
UPSERT item:2 SET n = 50 RETURN NONE;
UPSERT item:12 SET owner = user:alice, n = 12 RETURN NONE;
UPSERT item:13 SET owner = user:bob, n = 13 RETURN NONE;
INSERT INTO item { id: item:14, owner: user:bob, n: 14 } RETURN NONE;
INSERT INTO item { id: item:14, owner: user:bob, n: 15 } ON DUPLICATE KEY UPDATE n = 16 RETURN NONE;
INSERT INTO item { id: item:15, owner: user:alice, n: 17 } ON DUPLICATE KEY UPDATE n = 18 RETURN NONE;
RELATE item:1->link:1->item:3 SET n = 1 RETURN NONE;
RELATE item:1->link:2->item:2 SET n = 500 RETURN NONE;
UPDATE link SET n = n + 1 RETURN NONE;
DELETE item:3 RETURN NONE;
DELETE item:2 RETURN NONE;
DELETE item WHERE n > 1000 RETURN NONE;
DELETE link:2 RETURN NONE;
SELECT * FROM item_state ORDER BY id;
SELECT * FROM link_state ORDER BY id;
SELECT * FROM item ORDER BY id;
