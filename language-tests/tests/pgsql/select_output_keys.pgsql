/**
[env]
namespace = true
database = true
imports = ["pgsql/_output_keys.surql"]

[test]
reason = "A column that shares its name with another, or has none, is held under a field of its own, so every value keeps its column"

[[test.results]]
value = '[{ "?column?1": 1, "?column?2": 1, w: 100 }, { "?column?1": 1, "?column?2": 1, w: 200 }, { "?column?1": 2, "?column?2": 2, w: 300 }]'

[[test.results]]
value = """[{ "?column?1": 1, "?column?2": 1, v: 'x' }, { "?column?1": 1, "?column?2": 1, v: 'x' }, { "?column?1": 2, "?column?2": 2, v: NULL }, { "?column?1": 3, "?column?2": NONE, v: 'z' }]"""

[[test.results]]
value = """[{ "?column?1": 1, "?column?2": 'x' }, { "?column?1": 2, "?column?2": NULL }, { "?column?1": 3, "?column?2": 'z' }]"""

[[test.results]]
value = """[{ "?column?1": 3, "?column?2": 3, v: 'z' }, { "?column?1": 2, "?column?2": 2, v: NULL }]"""

[[test.results]]
value = """[{ "?column?1": 1, "?column?2": 'a', "?column?3": NULL, "?column?4": 5 }]"""

[[test.results]]
value = '[{ "?column?1": 1, "?column?2": 1, "?column?3": 1 }]'

[[test.results]]
value = '[{ "?column?1": 2, "?column?2": 2 }, { "?column?1": 3, "?column?2": 3 }, { "?column?1": 4, "?column?2": 4 }]'

[[test.results]]
value = '[{ "?column?1": 1, "?column?2": 1, "?column?3": 1, "?column?4": 1 }, { "?column?1": 1, "?column?2": 2, "?column?3": 1, "?column?4": 1 }, { "?column?1": 2, "?column?2": 3, "?column?3": 2, "?column?4": 2 }]'

[[test.results]]
value = '[{ "?column?1": 3, "?column?2": 2, "?column?3": 2, sum: 30 }]'
*/

SELECT a.k, b.k, b.w FROM okey_a a JOIN okey_b b ON a.k = b.k ORDER BY b.w;
SELECT a.k, b.k, a.v FROM okey_a a LEFT JOIN okey_b b ON a.k = b.k ORDER BY a.k, b.w;
SELECT k AS n, v AS n FROM okey_a ORDER BY k;
SELECT k, k, v FROM okey_a ORDER BY k DESC LIMIT 2;
SELECT 1, 'a', NULL, 2 + 3;
SELECT 1, 1, 1;
SELECT k + 1, k + 1 FROM okey_a ORDER BY k;
SELECT a.id, b.id, a.k, b.k FROM okey_a a JOIN okey_b b ON a.k = b.k ORDER BY b.id;
SELECT count(*), count(v), count(n), sum(n) FROM okey_a;
