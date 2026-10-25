/**
[test]
reason = "Records a CREATE .. RETURN NONE writes read back in the same transaction, and its table's events see each record."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:1, n: 1 }, { id: t:2, n: 1 }, { id: t:3, n: 1 }]"

[[test.results]]
value = "{ id: t:2, n: 1 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: e:1, seen: [{ id: e:1 }] }, { id: e:2, seen: [{ id: e:2 }] }]"

*/

-- Records a `CREATE .. RETURN NONE` writes read back in the same transaction,
-- by a scan and by key, and the events of the table see each record created.
BEGIN;
CREATE |t:1..4| SET n = 1 RETURN NONE;
SELECT * FROM t;
SELECT * FROM ONLY t:2;
COMMIT;
DEFINE EVENT seen ON e WHEN $event = "CREATE" THEN (UPDATE $after.id SET seen = (SELECT id FROM e WHERE id = $after.id));
CREATE |e:1..3| RETURN NONE;
SELECT * FROM e;
