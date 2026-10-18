/**
[env]
namespace = true
database = true
timeout = 60000

[test]
reason = "the counts of one group add to the serial run's totals, whatever order the scan's parts finish in"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 1000 }]"

[[test.results]]
value = "[{ c: 1449, d: 2899 }]"

[[test.results]]
value = "[{ c: 0 }]"

[[test.results]]
value = "[{ c: 429, hi: 2996 }]"
*/

DEFINE TABLE t;
FOR $i IN 0..3000 { CREATE type::record('t', $i) SET n = $i, ok = $i % 2 = 0 };
SELECT count() FROM t WHERE n % 3 = 0 GROUP ALL;
SELECT count(ok) AS c, count() AS d FROM t WHERE n > 100 GROUP ALL;
SELECT count() AS c FROM t WHERE n > 100000 GROUP ALL;
SELECT count() AS c, math::max(n) AS hi FROM t WHERE math::abs(n) % 7 = 0 GROUP ALL;
