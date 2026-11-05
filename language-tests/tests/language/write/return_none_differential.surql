/**
[env]
imports = ["./return_none_differential_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
`RETURN NONE` and `RETURN AFTER` run the same stages over a record, and differ
only in what the statement shows. Twin tables, one written with each output as
the same record user, store the same rows, fire their events for the same
records and actions, and answer the same errors, whether the select permission
reads a computed field or the record is one it hides. The change feed is not read:
`SHOW CHANGES` is refused to a record user, and the stage that records it runs
before the select gate in every order (see the
`nothing_but_the_output_follows_the_select_gate` unit test), so the gate cannot
change it.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ d: 2, id: b:1, n: 1, tag: 't1' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ d: 12, id: b:1, n: 6, tag: 't1' }, { d: 28, id: b:9, n: 14, tag: 'seed' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ d: 6, id: b:3, n: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ d: 12, id: b:1, n: 6, tag: 'upserted' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ d: 8, id: b:4, n: 4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ d: 82, id: b:4, n: 41 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[NONE, NONE]"

[[test.results]]
value = "[{ n: 6, rid: 1, tag: 'upserted' }, { n: 3, rid: 3, tag: NONE }]"

[[test.results]]
value = "[{ n: 6, rid: 1, tag: 'upserted' }, { n: 3, rid: 3, tag: NONE }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ event: 'CREATE', n: 1, rid: 1 }, { event: 'UPDATE', n: 6, rid: 1 }, { event: 'UPDATE', n: 6, rid: 1 }, { event: 'CREATE', n: 2000, rid: 2 }, { event: 'DELETE', n: 2005, rid: 2 }, { event: 'UPDATE', n: 2005, rid: 2 }, { event: 'CREATE', n: 3, rid: 3 }, { event: 'CREATE', n: 4, rid: 4 }, { event: 'DELETE', n: 41, rid: 4 }, { event: 'UPDATE', n: 41, rid: 4 }, { event: 'CREATE', n: 9, rid: 9 }, { event: 'DELETE', n: 14, rid: 9 }, { event: 'UPDATE', n: 14, rid: 9 }]"

[[test.results]]
value = "[{ event: 'CREATE', n: 1, rid: 1 }, { event: 'UPDATE', n: 6, rid: 1 }, { event: 'UPDATE', n: 6, rid: 1 }, { event: 'CREATE', n: 2000, rid: 2 }, { event: 'DELETE', n: 2005, rid: 2 }, { event: 'UPDATE', n: 2005, rid: 2 }, { event: 'CREATE', n: 3, rid: 3 }, { event: 'CREATE', n: 4, rid: 4 }, { event: 'DELETE', n: 41, rid: 4 }, { event: 'UPDATE', n: 41, rid: 4 }, { event: 'CREATE', n: 9, rid: 9 }, { event: 'DELETE', n: 14, rid: 9 }, { event: 'UPDATE', n: 14, rid: 9 }]"

[[test.results]]
value = "true"
*/

CREATE a:1 SET n = 1, tag = 't1' RETURN NONE;
CREATE b:1 SET n = 1, tag = 't1' RETURN AFTER;
CREATE a:2 SET n = 2000, tag = 'hidden' RETURN NONE;
CREATE b:2 SET n = 2000, tag = 'hidden' RETURN AFTER;
UPDATE a SET n = n + 5 RETURN NONE;
UPDATE b SET n = n + 5 RETURN AFTER;
UPSERT a:3 SET n = 3 RETURN NONE;
UPSERT b:3 SET n = 3 RETURN AFTER;
UPSERT a:1 SET tag = 'upserted' RETURN NONE;
UPSERT b:1 SET tag = 'upserted' RETURN AFTER;
INSERT INTO a { id: a:4, n: 4 } RETURN NONE;
INSERT INTO b { id: b:4, n: 4 } RETURN AFTER;
INSERT INTO a { id: a:4, n: 40 } ON DUPLICATE KEY UPDATE n = 41 RETURN NONE;
INSERT INTO b { id: b:4, n: 40 } ON DUPLICATE KEY UPDATE n = 41 RETURN AFTER;
DELETE a:2 RETURN NONE;
DELETE b:2 RETURN AFTER;
DELETE a WHERE n > 7 RETURN NONE;
DELETE b WHERE n > 7 RETURN AFTER;
SELECT n, tag, record::id(id) AS rid FROM state_a ORDER BY rid;
SELECT n, tag, record::id(id) AS rid FROM state_b ORDER BY rid;
RETURN (SELECT n, tag, record::id(id) AS rid FROM state_a ORDER BY rid) = (SELECT n, tag, record::id(id) AS rid FROM state_b ORDER BY rid);
SELECT event, rid, n FROM audit WHERE src = 'a' ORDER BY rid, event, n;
SELECT event, rid, n FROM audit WHERE src = 'b' ORDER BY rid, event, n;
RETURN (SELECT event, rid, n FROM audit WHERE src = 'a' ORDER BY rid, event, n) = (SELECT event, rid, n FROM audit WHERE src = 'b' ORDER BY rid, event, n);
