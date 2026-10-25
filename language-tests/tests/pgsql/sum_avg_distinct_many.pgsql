/**
[env]
namespace = true
database = true
imports = ["pgsql/_distinct_sums_many.surql"]

[test]
reason = "SUM(DISTINCT x) and AVG(DISTINCT x) over thousands of distinct numbers, in one group and in several"

[[test.results]]
match = "$result[0].s == 4498500 && type::is_int($result[0].s) && $result[0].a == 1499.5 && type::is_float($result[0].a) && $result[0].n == 3000 && $result[0].n_rows == 9000"

[[test.results]]
value = "[{ a: 1499f, grp: 0, s: 2248500 }, { a: 1500f, grp: 1, s: 2250000 }]"
*/

SELECT sum(DISTINCT v) AS s, avg(DISTINCT v) AS a, count(DISTINCT v) AS n, count(*) AS n_rows FROM dsm_t;
SELECT grp, sum(DISTINCT v) AS s, avg(DISTINCT v) AS a FROM dsm_t GROUP BY grp ORDER BY grp;
