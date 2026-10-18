/**
[test]
reason = "A view column is the group key its alias names in GROUP BY, whatever the other columns are called. In `SELECT g1 AS flag, g0 AS g1 ... GROUP BY flag, g1` the first column's expression is the path g1, which is also what the second column's alias is called. The second column is still the group `g1`: the view stores each group under the keys its columns name, and a source row that changes or leaves finds the group it was in. The views are defined before the rows that they hold (maintained row by row) and after them (filled in one go) and must hold the same, correct, groups."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 1, flag: false, g1: 'x', id: live_flag:[false, 'x'] }, { c: 2, flag: true, g1: 'a', id: live_flag:[true, 'a'] }]"

[[test.results]]
value = "[{ c: 1, flag: false, g1: 'x', id: late_flag:[false, 'x'] }, { c: 2, flag: true, g1: 'a', id: late_flag:[true, 'a'] }]"

[[test.results]]
value = "[{ g0: false, g1: 'x', id: live_swap:[false, 'x'], s: 4f }, { g0: true, g1: 'a', id: live_swap:[true, 'a'], s: 3f }]"

[[test.results]]
value = "[{ g0: false, g1: 'x', id: late_swap:[false, 'x'], s: 4f }, { g0: true, g1: 'a', id: late_swap:[true, 'a'], s: 3f }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c: 1, flag: true, g1: 'a', id: live_flag:[true, 'a'] }, { c: 1, flag: true, g1: 'b', id: live_flag:[true, 'b'] }]"

[[test.results]]
value = "[{ c: 1, flag: true, g1: 'a', id: late_flag:[true, 'a'] }, { c: 1, flag: true, g1: 'b', id: late_flag:[true, 'b'] }]"

[[test.results]]
value = "[{ g0: true, g1: 'a', id: live_swap:[true, 'a'], s: 2f }, { g0: true, g1: 'b', id: live_swap:[true, 'b'], s: 1f }]"

[[test.results]]
value = "[{ g0: true, g1: 'a', id: late_swap:[true, 'a'], s: 2f }, { g0: true, g1: 'b', id: late_swap:[true, 'b'], s: 1f }]"
*/

DEFINE TABLE gc SCHEMALESS;

-- Defined before any row exists.
DEFINE TABLE live_flag AS SELECT g1 AS flag, g0 AS g1, count() AS c FROM gc GROUP BY flag, g1;
DEFINE TABLE live_swap AS SELECT g1 AS g0, g0 AS g1, math::sum(n) AS s FROM gc GROUP BY g0, g1;

CREATE gc:1 SET g0 = 'a', g1 = true, n = 1 RETURN NONE;
CREATE gc:2 SET g0 = 'a', g1 = true, n = 2 RETURN NONE;
CREATE gc:3 SET g0 = 'x', g1 = false, n = 4 RETURN NONE;

-- Defined over the rows above.
DEFINE TABLE late_flag AS SELECT g1 AS flag, g0 AS g1, count() AS c FROM gc GROUP BY flag, g1;
DEFINE TABLE late_swap AS SELECT g1 AS g0, g0 AS g1, math::sum(n) AS s FROM gc GROUP BY g0, g1;

SELECT * FROM live_flag ORDER BY id;
SELECT * FROM late_flag ORDER BY id;
SELECT * FROM live_swap ORDER BY id;
SELECT * FROM late_swap ORDER BY id;

-- A row moves to a new group, and the last row of a group leaves.
UPDATE gc:1 SET g0 = 'b' RETURN NONE;
DELETE gc:3 RETURN NONE;

SELECT * FROM live_flag ORDER BY id;
SELECT * FROM late_flag ORDER BY id;
SELECT * FROM live_swap ORDER BY id;
SELECT * FROM late_swap ORDER BY id;
