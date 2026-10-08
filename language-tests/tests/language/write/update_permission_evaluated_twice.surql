/**
[env]
imports = ["language/write/update_permission_evaluated_twice_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "A `PERMISSIONS FOR update` predicate is judged exactly twice per record an update-shaped statement edits — once against the record as found, once against the record as edited — and that count is the contract, because the predicate may write. All four paths that can edit an existing record agree on it: UPDATE, UPSERT, INSERT ... ON DUPLICATE KEY UPDATE, and a RELATE onto an edge that already exists. A RELATE that creates its edge judges the update clause not at all."

[[test.results]]
value = "[{ id: counter:1, n: 0 }]"

[[test.results]]
value = "[{ id: thing:1, x: 2 }]"

[[test.results]]
value = "2"

[[test.results]]
value = "[{ id: counter:1, n: 0 }]"

[[test.results]]
value = "[{ id: thing:1, x: 3 }]"

[[test.results]]
value = "2"

[[test.results]]
value = "[{ id: counter:1, n: 0 }]"

[[test.results]]
value = "[{ id: thing:1, x: 4 }]"

[[test.results]]
value = "2"

[[test.results]]
value = "[{ id: counter:1, n: 0 }]"

[[test.results]]
value = "[{ id: edge:e1, in: person:bob, out: person:bob, x: 1 }]"

[[test.results]]
value = "0"

[[test.results]]
value = "[{ id: counter:1, n: 0 }]"

[[test.results]]
value = "[{ id: edge:e1, in: person:bob, out: person:bob, x: 2 }]"

[[test.results]]
value = "2"
*/

UPDATE counter:1 SET n = 0;
UPDATE thing:1 SET x = 2;
SELECT VALUE n FROM ONLY counter:1;

UPDATE counter:1 SET n = 0;
UPSERT thing:1 SET x = 3;
SELECT VALUE n FROM ONLY counter:1;

UPDATE counter:1 SET n = 0;
INSERT INTO thing { id: thing:1, x: 4 } ON DUPLICATE KEY UPDATE x = 4;
SELECT VALUE n FROM ONLY counter:1;

-- Creating the edge is not an update, and judges the clause no times.
UPDATE counter:1 SET n = 0;
RELATE person:bob->edge:e1->person:bob SET x = 1;
SELECT VALUE n FROM ONLY counter:1;

-- Relating onto the edge that now exists is an update, and judges it twice.
UPDATE counter:1 SET n = 0;
RELATE person:bob->edge:e1->person:bob SET x = 2;
SELECT VALUE n FROM ONLY counter:1;
