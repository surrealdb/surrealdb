/**
[env]
namespace = true
database = true
imports = ["pgsql/_output_keys.surql"]

[test]
reason = "The columns of an aggregation that repeat a name or have none are held under fields of their own, which its HAVING and ORDER BY clauses read"

[[test.results]]
value = '[{ "?column?2": 1, "?column?3": 1, "?column?4": 1, k: 1 }, { "?column?2": 1, "?column?3": 0, "?column?4": 1, k: 2 }, { "?column?2": 1, "?column?3": 1, "?column?4": 0, k: 3 }]'

[[test.results]]
value = '[{ "?column?2": 1, "?column?3": 1, k: 1 }, { "?column?2": 1, "?column?3": 1, k: 3 }]'

[[test.results]]
value = '[{ "?column?1": 0, "?column?2": 1, "?column?3": 1 }, { "?column?1": 1, "?column?2": 2, "?column?3": 2 }]'

[[test.results]]
value = '[{ "?column?1": 31, "?column?2": 32 }]'

[[test.results]]
value = '[{ "?column?1": 1, "?column?2": 1 }, { "?column?1": 2, "?column?2": 2 }, { "?column?1": 3, "?column?2": 3 }]'

[[test.results]]
value = '[{ "?column?1": 1, "?column?2": 1 }, { "?column?1": 2, "?column?2": 2 }, { "?column?1": 3, "?column?2": 3 }]'
*/

SELECT k, count(*), count(v), count(n) FROM okey_a GROUP BY k ORDER BY k;
SELECT k, count(*), count(v) FROM okey_a GROUP BY k HAVING count(v) > 0 ORDER BY count(*), k;
SELECT k % 2, count(*), count(*) FROM okey_a GROUP BY k % 2 ORDER BY 1;
SELECT sum(n) + 1, sum(n) + 2 FROM okey_a;
SELECT k, k FROM okey_a GROUP BY k ORDER BY k;
SELECT a.id, a.id FROM okey_a a GROUP BY a.id ORDER BY a.id;
