/**
[env]
namespace = true
database = true
imports = ["pgsql/_distinct_sums.surql"]

[test]
reason = "SUM(DISTINCT x) and AVG(DISTINCT x) add and average the distinct values of x that are not NULL: repeats, NULLs and missing values are left out, a group with no such value answers NULL, values that cancel sum to 0 and not NULL, decimals sum exactly and average to a float, and the aggregates compose with other expressions in ORDER BY, CASE and FILTER"

[[test.results]]
value = "[{ sum_distinct: 4, sum_all: 8, avg_distinct: 2f }]"

[[test.results]]
value = "[{ avg_distinct: 2f, grp: 'x', sum_distinct: 4 }, { avg_distinct: 3f, grp: 'y', sum_distinct: 3 }, { avg_distinct: NULL, grp: 'z', sum_distinct: NULL }]"

[[test.results]]
value = "[{ avg_distinct: NULL, sum_distinct: NULL }]"

[[test.results]]
value = "[{ grp: 'x' }, { grp: 'y' }]"

[[test.results]]
value = "[{ total: 4, w_sum: 6.5f }]"

[[test.results]]
value = "[{ shifted: 14 }]"

[[test.results]]
value = "[{ a: 0f, s: 0 }]"

[[test.results]]
value = "[{ a: 2f, s: 4dec }]"

[[test.results]]
value = "[{ grp: 'x' }, { grp: 'y' }, { grp: 'z' }]"

[[test.results]]
value = "[{ size: 'big' }]"

[[test.results]]
value = "[{ s: 4 }]"
*/

SELECT sum(DISTINCT v) AS sum_distinct, sum(v) AS sum_all, avg(DISTINCT v) AS avg_distinct FROM ds_t;
SELECT grp, sum(DISTINCT v) AS sum_distinct, avg(DISTINCT v) AS avg_distinct FROM ds_t GROUP BY grp ORDER BY grp;
SELECT sum(DISTINCT v) AS sum_distinct, avg(DISTINCT v) AS avg_distinct FROM ds_t WHERE k > 100;
SELECT grp FROM ds_t GROUP BY grp HAVING sum(DISTINCT v) > 2 ORDER BY grp;
SELECT sum(DISTINCT v) AS total, sum(DISTINCT w) AS w_sum FROM ds_t;
SELECT sum(DISTINCT v) * 2 + 6 AS shifted FROM ds_t;
SELECT sum(DISTINCT v) AS s, avg(DISTINCT v) AS a FROM ds_z;
SELECT sum(DISTINCT v) AS s, avg(DISTINCT v) AS a FROM ds_d;
SELECT grp FROM ds_t GROUP BY grp ORDER BY sum(DISTINCT v) DESC NULLS LAST, grp;
SELECT CASE WHEN sum(DISTINCT v) > 3 THEN 'big' ELSE 'small' END AS size FROM ds_t;
SELECT sum(DISTINCT v) FILTER (WHERE grp = 'x') AS s FROM ds_t;
