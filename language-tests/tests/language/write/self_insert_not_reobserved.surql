/**
[env]
namespace = true
database = true
timeout = 15000

[test]
issue = 1669
reason = """
A record created by a statement is not visited by that same statement's scan, in
either key direction, whatever the table's size. The write path interleaves
reads and writes batch-at-a-time instead of draining its input, and every batch
is read through the statement's own transaction, so a scan that spans more than
one batch would otherwise read a record the statement created ahead of its
cursor. The two-record tables fit in one batch; the 1,500-record tables span two
batches of the scan. Each case uses its own table so no statement can observe
another's records.
"""

[[test.results]]
value = "[{ id: ahead:1 }]"

[[test.results]]
value = "[{ id: ahead:2 }]"

[[test.results]]
value = "[{ id: ahead:1, spawn: [ahead:9], touched: true }, { id: ahead:2, touched: true }]"

[[test.results]]
value = "[{ id: ahead:1, touched: true }, { id: ahead:2, touched: true }, { id: ahead:9, touched: NONE }]"

[[test.results]]
value = "[{ id: behind:5 }]"

[[test.results]]
value = "[{ id: behind:6 }]"

[[test.results]]
value = "[{ id: behind:5, spawn: [behind:4], touched: true }, { id: behind:6, touched: true }]"

[[test.results]]
value = "[{ id: behind:4, touched: NONE }, { id: behind:5, touched: true }, { id: behind:6, touched: true }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 1500 }]"

[[test.results]]
value = "[big_ahead:999999]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 1500 }]"

[[test.results]]
value = "[big_behind:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 1500 }]"

[[test.results]]
value = "[big_where:999999]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 1500 }]"

[[test.results]]
value = "[big_index:999999]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 1500 }]"

[[test.results]]
value = "[big_upsert:999999]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[big_delete:999999]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 3000 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: lwv:0 }, { id: lwv:2000 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[lwv:2000, lwv:0]]"
*/

-- Ahead of the cursor: ahead:9 sorts after both scanned records.
CREATE ahead:1;
CREATE ahead:2;
UPDATE ahead SET touched = true, spawn = (IF id = ahead:1 { (CREATE ahead:9).id } ELSE { NONE });
SELECT id, touched FROM ahead ORDER BY id;

-- Behind the cursor: behind:4 sorts before both scanned records.
CREATE behind:5;
CREATE behind:6;
UPDATE behind SET touched = true, spawn = (IF id = behind:5 { (CREATE behind:4).id } ELSE { NONE });
SELECT id, touched FROM behind ORDER BY id;

-- Ahead of the cursor, past the scan's first batch: big_ahead:999999 is
-- created while the first batch is written and sorts into the second.
FOR $i IN 1..=1500 { CREATE type::record('big_ahead', $i) };
UPDATE big_ahead SET touched = true, spawn = (IF id = big_ahead:1 { (CREATE big_ahead:999999).id } ELSE { NONE }) RETURN NONE;
SELECT count() AS n FROM big_ahead WHERE touched GROUP ALL;
SELECT VALUE id FROM big_ahead WHERE touched = NONE;

-- Behind the cursor, from the scan's last batch: big_behind:1 is created
-- once the cursor has passed the batch it sorts into.
FOR $i IN 2..=1501 { CREATE type::record('big_behind', $i) };
UPDATE big_behind SET touched = true, spawn = (IF id = big_behind:1501 { (CREATE big_behind:1).id } ELSE { NONE }) RETURN NONE;
SELECT count() AS n FROM big_behind WHERE touched GROUP ALL;
SELECT VALUE id FROM big_behind WHERE touched = NONE;

-- A condition reads the table's records through the statement's pipeline
-- rather than as a target.
FOR $i IN 1..=1500 { CREATE type::record('big_where', $i) SET n = $i };
UPDATE big_where SET touched = true, spawn = (IF id = big_where:1 { (CREATE big_where:999999 SET n = 999999).id } ELSE { NONE }) WHERE n >= 1 RETURN NONE;
SELECT count() AS n FROM big_where WHERE touched GROUP ALL;
SELECT VALUE id FROM big_where WHERE touched = NONE;

-- An index answers the condition: the created record's entry sorts after
-- every entry the first batch read.
DEFINE INDEX by_n ON big_index FIELDS n;
FOR $i IN 1..=1500 { CREATE type::record('big_index', $i) SET n = $i };
UPDATE big_index SET touched = true, spawn = (IF n = 1 { (CREATE big_index:999999 SET n = 999999).id } ELSE { NONE }) WHERE n >= 1 RETURN NONE;
SELECT count() AS n FROM big_index WHERE touched GROUP ALL;
SELECT VALUE id FROM big_index WHERE touched = NONE;

-- UPSERT with a condition reads the table's records as UPDATE does.
FOR $i IN 1..=1500 { CREATE type::record('big_upsert', $i) SET n = $i };
UPSERT big_upsert SET touched = true, spawn = (IF id = big_upsert:1 { (CREATE big_upsert:999999 SET n = 999999).id } ELSE { NONE }) WHERE n >= 1 RETURN NONE;
SELECT count() AS n FROM big_upsert WHERE touched GROUP ALL;
SELECT VALUE id FROM big_upsert WHERE touched = NONE;

-- DELETE: the record an event of the statement's first delete creates
-- survives the statement.
DEFINE EVENT spawn ON big_delete WHEN $event = "DELETE" AND $before.id = big_delete:1 THEN { CREATE big_delete:999999 };
FOR $i IN 1..=1500 { CREATE type::record('big_delete', $i) };
DELETE big_delete RETURN NONE;
SELECT VALUE id FROM big_delete;

-- INSERT reads every row its SELECT names before it writes the first.
FOR $i IN 1..=1500 { CREATE type::record('big_insert', $i) };
INSERT INTO big_insert (SELECT VALUE { id: type::record('big_insert', record::id(id) + 1500) } FROM big_insert) RETURN NONE;
SELECT count() AS n FROM big_insert GROUP ALL;

-- A lightweight relation's edge is its adjacency keys alone. The edge the
-- DELETE relates survives it, and the edge it relates again, which the
-- relation already held, is deleted when the scan reaches it.
DEFINE TABLE lwv;
DEFINE TABLE lw TYPE RELATION IN lwv OUT lwv LIGHTWEIGHT;
CREATE lwv:0, lwv:2000;
FOR $i IN 1..=1100 { RELATE lwv:0->lw->(CREATE ONLY type::record('lwv', $i)) };
DELETE lw WHERE (IF out = lwv:1 { RELATE lwv:2000->lw->lwv:0; RELATE lwv:0->lw->lwv:1100; true } ELSE { true }) RETURN NONE;
SELECT VALUE [in, out] FROM lw;
