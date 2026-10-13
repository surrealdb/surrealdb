/**
[test]
reason = "An aggregated view defined over rows that already exist holds the same groups as the view defined before the rows were written, which is maintained as each row arrives, and keeps holding them as source rows are then updated and deleted. Each `late_*` view is defined over the existing rows and each `live_*` view before them, from the same select. The group columns are named g0, g1 and g2, which the select that fills a view also names its group keys, in the order they are grouped and swapped; they are of every type a key can be; and the views are filtered by WHERE, grouped by nothing (GROUP ALL), defined over a table that holds no rows, and defined again over the rows they were filled from."

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
value = "NONE"

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
value = "[{ c: 1, g0: 'a', g1: false, g2: 1, id: late_names:['a', false, 1], s: 5f }, { c: 2, g0: 'a', g1: true, g2: 1, id: late_names:['a', true, 1], s: 30f }, { c: 2, g0: 'b', g1: true, g2: 2, id: late_names:['b', true, 2], s: 107f }]"

[[test.results]]
value = "[{ c: 1, g0: 'a', g1: false, id: late_swapped:[false, 'a'] }, { c: 2, g0: 'a', g1: true, id: late_swapped:[true, 'a'] }, { c: 2, g0: 'b', g1: true, id: late_swapped:[true, 'b'] }]"

[[test.results]]
value = "[{ c: 1, g1: false, g2: 1, id: late_where:[false, 1] }, { c: 2, g1: true, g2: 1, id: late_where:[true, 1] }, { c: 1, g1: true, g2: 2, id: late_where:[true, 2] }]"

[[test.results]]
value = "[{ c: 5, hi: 100, id: late_all:[], s: 142f }]"

[[test.results]]
value = "[{ c: 2, id: late_types:['a', true, 1, 1.5f, d'2024-01-01T00:00:00Z', other:1, [1, 2], { x: 1 }, NONE], k_arr: [1, 2], k_bool: true, k_dt: d'2024-01-01T00:00:00Z', k_flt: 1.5f, k_int: 1, k_none: NONE, k_obj: { x: 1 }, k_rec: other:1, k_str: 'a' }, { c: 1, id: late_types:['b', false, 2, 2.5f, d'2025-01-01T00:00:00Z', other:2, [3], { x: 2 }, NONE], k_arr: [3], k_bool: false, k_dt: d'2025-01-01T00:00:00Z', k_flt: 2.5f, k_int: 2, k_none: NONE, k_obj: { x: 2 }, k_rec: other:2, k_str: 'b' }]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

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
value = "[{ c: 1, g0: 'a', g1: false, g2: 1, id: late_names:['a', false, 1], s: 10f }, { c: 1, g0: 'a', g1: true, g2: 1, id: late_names:['a', true, 1], s: 21f }, { c: 1, g0: 'b', g1: true, g2: 2, id: late_names:['b', true, 2], s: 1f }, { c: 1, g0: 'c', g1: false, g2: 3, id: late_names:['c', false, 3], s: 3f }]"

[[test.results]]
value = "[{ c: 1, g0: 'a', g1: false, id: late_swapped:[false, 'a'] }, { c: 1, g0: 'c', g1: false, id: late_swapped:[false, 'c'] }, { c: 1, g0: 'a', g1: true, id: late_swapped:[true, 'a'] }, { c: 1, g0: 'b', g1: true, id: late_swapped:[true, 'b'] }]"

[[test.results]]
value = "[{ c: 1, g1: false, g2: 1, id: late_where:[false, 1] }, { c: 1, g1: false, g2: 3, id: late_where:[false, 3] }, { c: 1, g1: true, g2: 1, id: late_where:[true, 1] }, { c: 1, g1: true, g2: 2, id: late_where:[true, 2] }]"

[[test.results]]
value = "[{ c: 4, hi: 21, id: late_all:[], s: 35f }]"

[[test.results]]
value = "[{ c: 1, id: late_types:['a', true, 1, 1.5f, d'2024-01-01T00:00:00Z', other:1, [1, 2], { x: 1 }, NONE], k_arr: [1, 2], k_bool: true, k_dt: d'2024-01-01T00:00:00Z', k_flt: 1.5f, k_int: 1, k_none: NONE, k_obj: { x: 1 }, k_rec: other:1, k_str: 'a' }, { c: 1, id: late_types:['b', false, 2, 2.5f, d'2025-01-01T00:00:00Z', other:2, [4], { x: 2 }, NONE], k_arr: [4], k_bool: false, k_dt: d'2025-01-01T00:00:00Z', k_flt: 2.5f, k_int: 2, k_none: NONE, k_obj: { x: 2 }, k_rec: other:2, k_str: 'b' }]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c: 1, g0: 'a', g1: false, id: empty_names:['a', false] }, { c: 2, g0: 'a', g1: true, id: empty_names:['a', true] }]"

[[test.results]]
value = "[{ c: 3, id: empty_all:[] }]"

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
value = "[{ b: 'x', c: 2, hi: 2, id: late_a:['x'], s: 3f }, { b: 'y', c: 1, hi: 2, id: late_a:['y'], s: 2f }]"

[[test.results]]
value = "[{ a: 1, b: 'x', id: late_ab:[1, 'x'], s: 10f }, { a: 2, b: 'x', id: late_ab:[2, 'x'], s: 20f }, { a: 2, b: 'y', id: late_ab:[2, 'y'], s: 30f }]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 1, g1: false, g2: 3, id: late_where:[false, 3] }, { c: 1, g1: true, g2: 2, id: late_where:[true, 2] }]"
*/

DEFINE TABLE gc SCHEMALESS;
DEFINE TABLE ty SCHEMALESS;
DEFINE TABLE nobody SCHEMALESS;

-- The live twins, defined before any source row exists.
DEFINE TABLE live_names AS SELECT g0, g1, g2, count() AS c, math::sum(n) AS s FROM gc GROUP BY g0, g1, g2;
DEFINE TABLE live_swapped AS SELECT g1, g0, count() AS c FROM gc GROUP BY g1, g0;
DEFINE TABLE live_where AS SELECT g1, g2, count() AS c FROM gc WHERE n < 50 GROUP BY g1, g2;
DEFINE TABLE live_all AS SELECT count() AS c, math::sum(n) AS s, math::max(n) AS hi FROM gc GROUP ALL;
DEFINE TABLE live_types AS SELECT k_str, k_bool, k_int, k_flt, k_dt, k_rec, k_arr, k_obj, k_none, count() AS c FROM ty GROUP BY k_str, k_bool, k_int, k_flt, k_dt, k_rec, k_arr, k_obj, k_none;

CREATE gc:1 SET g0 = 'a', g1 = true, g2 = 1, n = 10 RETURN NONE;
CREATE gc:2 SET g0 = 'a', g1 = true, g2 = 1, n = 20 RETURN NONE;
CREATE gc:3 SET g0 = 'a', g1 = false, g2 = 1, n = 5 RETURN NONE;
CREATE gc:4 SET g0 = 'b', g1 = true, g2 = 2, n = 7 RETURN NONE;
CREATE gc:5 SET g0 = 'b', g1 = true, g2 = 2, n = 100 RETURN NONE;
CREATE ty:1 SET k_str = 'a', k_bool = true, k_int = 1, k_flt = 1.5f, k_dt = d'2024-01-01T00:00:00Z', k_rec = other:1, k_arr = [1, 2], k_obj = { x: 1 } RETURN NONE;
CREATE ty:2 SET k_str = 'a', k_bool = true, k_int = 1, k_flt = 1.5f, k_dt = d'2024-01-01T00:00:00Z', k_rec = other:1, k_arr = [1, 2], k_obj = { x: 1 } RETURN NONE;
CREATE ty:3 SET k_str = 'b', k_bool = false, k_int = 2, k_flt = 2.5f, k_dt = d'2025-01-01T00:00:00Z', k_rec = other:2, k_arr = [3], k_obj = { x: 2 } RETURN NONE;

-- The late twins, defined over the rows above.
DEFINE TABLE late_names AS SELECT g0, g1, g2, count() AS c, math::sum(n) AS s FROM gc GROUP BY g0, g1, g2;
DEFINE TABLE late_swapped AS SELECT g1, g0, count() AS c FROM gc GROUP BY g1, g0;
DEFINE TABLE late_where AS SELECT g1, g2, count() AS c FROM gc WHERE n < 50 GROUP BY g1, g2;
DEFINE TABLE late_all AS SELECT count() AS c, math::sum(n) AS s, math::max(n) AS hi FROM gc GROUP ALL;
DEFINE TABLE late_types AS SELECT k_str, k_bool, k_int, k_flt, k_dt, k_rec, k_arr, k_obj, k_none, count() AS c FROM ty GROUP BY k_str, k_bool, k_int, k_flt, k_dt, k_rec, k_arr, k_obj, k_none;

-- What each late view holds, then whether its live twin holds the same.
SELECT * FROM late_names ORDER BY id;
SELECT * FROM late_swapped ORDER BY id;
SELECT * FROM late_where ORDER BY id;
SELECT * FROM late_all;
SELECT * FROM late_types ORDER BY id;
(SELECT g0, g1, g2, c, s FROM live_names ORDER BY g0, g1, g2) = (SELECT g0, g1, g2, c, s FROM late_names ORDER BY g0, g1, g2);
(SELECT g1, g0, c FROM live_swapped ORDER BY g1, g0) = (SELECT g1, g0, c FROM late_swapped ORDER BY g1, g0);
(SELECT g1, g2, c FROM live_where ORDER BY g1, g2) = (SELECT g1, g2, c FROM late_where ORDER BY g1, g2);
(SELECT c, s, hi FROM live_all) = (SELECT c, s, hi FROM late_all);
(SELECT k_str, k_bool, k_int, k_flt, k_dt, k_rec, k_arr, k_obj, k_none, c FROM live_types ORDER BY k_str) = (SELECT k_str, k_bool, k_int, k_flt, k_dt, k_rec, k_arr, k_obj, k_none, c FROM late_types ORDER BY k_str);

-- Source rows change after the build: a row moves between groups, one changes
-- what it adds to its group, one crosses the WHERE, one leaves, one arrives in
-- a new group, and the last row of a group leaves.
UPDATE gc:1 SET g1 = false RETURN NONE;
UPDATE gc:2 SET n = 21 RETURN NONE;
UPDATE gc:5 SET n = 1 RETURN NONE;
DELETE gc:3 RETURN NONE;
CREATE gc:6 SET g0 = 'c', g1 = false, g2 = 3, n = 3 RETURN NONE;
DELETE gc:4 RETURN NONE;
DELETE ty:2 RETURN NONE;
UPDATE ty:3 SET k_arr = [4] RETURN NONE;

SELECT * FROM late_names ORDER BY id;
SELECT * FROM late_swapped ORDER BY id;
SELECT * FROM late_where ORDER BY id;
SELECT * FROM late_all;
SELECT * FROM late_types ORDER BY id;
(SELECT g0, g1, g2, c, s FROM live_names ORDER BY g0, g1, g2) = (SELECT g0, g1, g2, c, s FROM late_names ORDER BY g0, g1, g2);
(SELECT g1, g0, c FROM live_swapped ORDER BY g1, g0) = (SELECT g1, g0, c FROM late_swapped ORDER BY g1, g0);
(SELECT g1, g2, c FROM live_where ORDER BY g1, g2) = (SELECT g1, g2, c FROM late_where ORDER BY g1, g2);
(SELECT c, s, hi FROM live_all) = (SELECT c, s, hi FROM late_all);
(SELECT k_str, k_bool, k_int, k_flt, k_dt, k_rec, k_arr, k_obj, k_none, c FROM live_types ORDER BY k_str) = (SELECT k_str, k_bool, k_int, k_flt, k_dt, k_rec, k_arr, k_obj, k_none, c FROM late_types ORDER BY k_str);

-- Defined over a table with no rows, a view holds nothing until a row arrives.
DEFINE TABLE empty_names AS SELECT g0, g1, count() AS c FROM nobody GROUP BY g0, g1;
DEFINE TABLE empty_all AS SELECT count() AS c FROM nobody GROUP ALL;
SELECT * FROM empty_names;
SELECT * FROM empty_all;
CREATE nobody:1 SET g0 = 'a', g1 = true RETURN NONE;
CREATE nobody:2 SET g0 = 'a', g1 = false RETURN NONE;
CREATE nobody:3 SET g0 = 'a', g1 = true RETURN NONE;
SELECT * FROM empty_names ORDER BY id;
SELECT * FROM empty_all;
DELETE nobody RETURN NONE;
SELECT * FROM empty_names;
SELECT * FROM empty_all;

-- A column named like the value the select that fills a view computes (a), and
-- an aggregate over a column named like a group key (g0).
DEFINE TABLE ak SCHEMALESS;
DEFINE TABLE live_a AS SELECT b, math::sum(a) AS s, math::max(a) AS hi, count() AS c FROM ak GROUP BY b;
DEFINE TABLE live_ab AS SELECT a, b, math::sum(g0) AS s FROM ak GROUP BY a, b;
CREATE ak:1 SET a = 1, b = 'x', g0 = 10 RETURN NONE;
CREATE ak:2 SET a = 2, b = 'x', g0 = 20 RETURN NONE;
CREATE ak:3 SET a = 2, b = 'y', g0 = 30 RETURN NONE;
DEFINE TABLE late_a AS SELECT b, math::sum(a) AS s, math::max(a) AS hi, count() AS c FROM ak GROUP BY b;
DEFINE TABLE late_ab AS SELECT a, b, math::sum(g0) AS s FROM ak GROUP BY a, b;
SELECT * FROM late_a ORDER BY id;
SELECT * FROM late_ab ORDER BY id;
(SELECT b, s, hi, c FROM live_a ORDER BY b) = (SELECT b, s, hi, c FROM late_a ORDER BY b);
(SELECT a, b, s FROM live_ab ORDER BY a, b) = (SELECT a, b, s FROM late_ab ORDER BY a, b);

-- Redefining a view over rows that exist fills it again from those rows: the
-- same definition leaves the same groups, and a definition that selects fewer
-- rows leaves no row behind for a group it no longer holds.
DEFINE TABLE OVERWRITE late_names AS SELECT g0, g1, g2, count() AS c, math::sum(n) AS s FROM gc GROUP BY g0, g1, g2;
(SELECT g0, g1, g2, c, s FROM live_names ORDER BY g0, g1, g2) = (SELECT g0, g1, g2, c, s FROM late_names ORDER BY g0, g1, g2);
DEFINE TABLE OVERWRITE late_where AS SELECT g1, g2, count() AS c FROM gc WHERE n < 5 GROUP BY g1, g2;
SELECT * FROM late_where ORDER BY id;
