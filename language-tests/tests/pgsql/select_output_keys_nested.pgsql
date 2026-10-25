/**
[env]
namespace = true
database = true
imports = ["pgsql/_output_keys.surql"]

[test]
reason = "A select read by an enclosing query keeps the names its columns have, and the columns of the enclosing select are keyed"

[[test.results]]
value = "[{ k: 1, w: 100 }, { k: 1, w: 200 }, { k: 2, w: 300 }]"

[[test.results]]
value = '[{ "?column?1": 1, "?column?2": 1 }, { "?column?1": 2, "?column?2": 2 }, { "?column?1": 3, "?column?2": 3 }]'

[[test.results]]
value = '[{ "?column?1": 1, "?column?2": 1 }, { "?column?1": 2, "?column?2": 2 }, { "?column?1": 3, "?column?2": 3 }]'

[[test.results]]
value = '[{ "?column?1": 300, "?column?2": 100 }]'

[[test.results]]
value = '[{ "?column?1": 1, "?column?2": 1 }, { "?column?1": 2, "?column?2": 2 }]'
*/

SELECT s.k, s.w FROM (SELECT a.k, b.w FROM okey_a a JOIN okey_b b ON a.k = b.k) s ORDER BY s.w;
SELECT x, x FROM (SELECT k AS x FROM okey_a) s ORDER BY x;
WITH c AS (SELECT k AS x FROM okey_a) SELECT x, x FROM c ORDER BY x;
SELECT (SELECT max(w) FROM okey_b), (SELECT min(w) FROM okey_b);
SELECT k, k FROM okey_a WHERE k IN (SELECT k FROM okey_b) ORDER BY k;
