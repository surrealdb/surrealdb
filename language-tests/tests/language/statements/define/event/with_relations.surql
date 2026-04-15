/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Events on vertex and edge tables fire correctly during RELATE, direct edge deletion, and cascade deletion from vertex removal"

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
value = "[{ id: vertex:1, name: 'a' }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: edge_tb:1, in: vertex:1, out: vertex:1 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: edge_tb:2, in: vertex:1, out: vertex:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "['DELETE']"

[[test.results]]
value = "['DELETE']"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: vertex:2, name: 'b' }]"

[[test.results]]
value = "[{ id: edge_tb:3, in: vertex:2, out: vertex:2 }]"

[[test.results]]
value = "[{ id: edge_tb:4, in: vertex:2, out: vertex:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ data: 'test', id: vertex:3 }]"

[[test.results]]
value = "[{ id: edge_tb:5, in: vertex:3, out: vertex:3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ data: 'updated', id: vertex:3 }]"

[[test.results]]
value = "['UPDATE']"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "['DELETE']"

[[test.results]]
value = "['DELETE']"
*/

DEFINE TABLE OVERWRITE vertex;
DEFINE TABLE OVERWRITE edge_tb;
DEFINE TABLE OVERWRITE vlog;
DEFINE TABLE OVERWRITE elog;

DEFINE EVENT OVERWRITE vev ON TABLE vertex WHEN $before != $after THEN ({
    CREATE vlog SET ref = $before.id || $after.id, action = $event;
});
DEFINE EVENT OVERWRITE eev ON TABLE edge_tb WHEN $before != $after THEN ({
    CREATE elog SET ref = $before.id || $after.id, action = $event;
});

-- Phase 1: Create vertex fires only vertex event
CREATE vertex:1 SET name = "a";
SELECT count() FROM vlog GROUP ALL;
SELECT count() FROM elog GROUP ALL;
DELETE vlog; DELETE elog;

-- Phase 2: Create relation fires only edge event (not vertex event)
RELATE vertex:1->edge_tb:1->vertex:1;
SELECT count() FROM vlog GROUP ALL;
SELECT count() FROM elog GROUP ALL;
DELETE vlog; DELETE elog;

-- Phase 3: Direct edge deletion fires only edge event
RELATE vertex:1->edge_tb:2->vertex:1;
DELETE vlog; DELETE elog;
DELETE edge_tb:2;
SELECT count() FROM vlog GROUP ALL;
SELECT count() FROM elog GROUP ALL;
DELETE vlog; DELETE elog;

-- Phase 4: Deleting a vertex fires vertex event for vertex,
-- and edge event for cascade cleanup of the connected edge
DELETE vertex:1;
SELECT count() FROM vlog GROUP ALL;
SELECT count() FROM elog GROUP ALL;
SELECT VALUE action FROM vlog;
SELECT VALUE action FROM elog;
DELETE vlog; DELETE elog; DELETE vertex; DELETE edge_tb;

-- Phase 5: Multiple edges from same vertex - vertex event fires once,
-- edge event fires once per cascaded edge
CREATE vertex:2 SET name = "b";
RELATE vertex:2->edge_tb:3->vertex:2;
RELATE vertex:2->edge_tb:4->vertex:2;
DELETE vlog; DELETE elog;
DELETE vertex:2;
SELECT count() FROM vlog GROUP ALL;
SELECT count() FROM elog GROUP ALL;

-- Phase 6: Verify $before/$after correctness for vertex events during create+delete
DELETE vlog; DELETE elog; DELETE vertex; DELETE edge_tb;
CREATE vertex:3 SET data = "test";
RELATE vertex:3->edge_tb:5->vertex:3;
DELETE vlog; DELETE elog;
UPDATE vertex:3 SET data = "updated";
SELECT VALUE action FROM vlog;
DELETE vlog;
DELETE vertex:3;
SELECT VALUE action FROM vlog;
SELECT VALUE action FROM elog;
