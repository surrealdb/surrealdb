/**
[test]

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
value = "NONE"

[[test.results]]
value = "[{ c: 0, d: 1, id: q:1, n: 5, t: 10 }]"

[[test.results]]
value = "[{ c: 5, d: 2, e: 3, id: q:2, n: 6, t: 10 }]"

[[test.results]]
value = "[{ c: 0, d: 1, id: q:1, n: 7, t: 14 }]"

[[test.results]]
value = "[{ c: 0, d: 9, id: q:1, n: 7, t: 14 }, { c: 5, d: 9, e: 3, id: q:2, n: 6, t: 14 }]"

[[test.results]]
value = "[{ c: 0, d: 9, id: q:1, n: 7, t: 14 }, { c: 5, d: 9, e: 3, id: q:2, n: 6, t: 14 }]"

*/

-- Field clauses that hold a statement or a call of a function, applied to the
-- records a write creates and updates.
DEFINE FUNCTION fn::open() { RETURN true } PERMISSIONS FULL;
DEFINE TABLE q PERMISSIONS FULL;
DEFINE FIELD n ON q TYPE int;
DEFINE FIELD c ON q TYPE option<int> DEFAULT (SELECT VALUE n FROM ONLY q:1) ?? 0;
DEFINE FIELD d ON q TYPE option<int> PERMISSIONS FOR create, update WHERE fn::open();
DEFINE FIELD e ON q TYPE option<int> ASSERT $value = NONE OR (SELECT VALUE true FROM ONLY q:1) = true;
DEFINE FIELD t ON q COMPUTED (SELECT VALUE n FROM ONLY q:1) * 2;
CREATE q:1 SET n = 5, d = 1;
CREATE q:2 SET n = 6, d = 2, e = 3 RETURN AFTER;
UPDATE q:1 SET n = 7 RETURN AFTER;
UPDATE q SET d = 9;
SELECT * FROM q ORDER BY id;
