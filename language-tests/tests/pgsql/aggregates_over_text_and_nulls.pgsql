/**
[env]
namespace = true
database = true
imports = ["pgsql/_aggregate_nulls.surql"]

[test]
reason = "min and max order any value, and sum, avg, min and max of no value are NULL, in a plain and in a grouped aggregate"

[[test.results]]
value = "[{ hi: 'dan', lo: 'ada' }]"

[[test.results]]
value = "[{ a: 32.5, hi: 36, lo: 29, s: 65 }]"

[[test.results]]
value = "[{ a: NULL, hi: NULL, lo: NULL, n: 0, s: NULL }]"

[[test.results]]
value = "[{ a: 36, grp: 'x', hi: 'bob', lo: 'ada', s: 36 }, { a: 29, grp: 'y', hi: 'cy', lo: 'cy', s: 29 }, { a: NULL, grp: 'z', hi: 'dan', lo: 'dan', s: NULL }]"

[[test.results]]
value = "[{ grp: 'x', hi: 36, lo: 36 }, { grp: 'y', hi: 29, lo: 29 }, { grp: 'z', hi: NULL, lo: NULL }]"

[[test.results]]
value = "[{ s: 0 }]"

[[test.results]]
value = "[{ grp: 'x', s: 36 }, { grp: 'y', s: 29 }, { grp: 'z', s: 0 }]"

[[test.results]]
value = "[{ grp: 'x' }]"

[[test.results]]
value = "[{ grp: 'x' }, { grp: 'y' }, { grp: 'z' }]"
*/

SELECT min(name) AS lo, max(name) AS hi FROM agg_t;
SELECT min(age) AS lo, max(age) AS hi, sum(age) AS s, avg(age) AS a FROM agg_t;
SELECT sum(age) AS s, avg(age) AS a, min(age) AS lo, max(name) AS hi, count(age) AS n FROM agg_t WHERE k > 100;
SELECT grp, min(name) AS lo, max(name) AS hi, sum(age) AS s, avg(age) AS a FROM agg_t GROUP BY grp ORDER BY grp;
SELECT grp, min(age) AS lo, max(age) AS hi FROM agg_t GROUP BY grp ORDER BY grp;
SELECT coalesce(sum(age), 0) AS s FROM agg_t WHERE k > 100;
SELECT grp, coalesce(sum(age), 0) AS s FROM agg_t GROUP BY grp ORDER BY grp;
SELECT grp FROM agg_t GROUP BY grp HAVING coalesce(sum(age), 0) > 30 ORDER BY grp;
SELECT grp FROM agg_t GROUP BY grp ORDER BY coalesce(sum(age), 0) DESC;
