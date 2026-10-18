/**
[env]
namespace = true
database = true
timeout = 30000

[test]
reason = "a projection, a sort, a top-k, a grouping, a paged scan, a backward scan and a record range answer the same whatever width their scan runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "600"

[[test.results]]
value = "899100"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3000"

[[test.results]]
value = "4498500"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2000"

[[test.results]]
value = "2999000"

[[test.results]]
value = "[{ k: 0, n: 0 }, { k: 0, n: 7 }, { k: 0, n: 14 }, { k: 0, n: 21 }, { k: 0, n: 28 }, { k: 0, n: 35 }, { k: 0, n: 42 }, { k: 0, n: 49 }]"

[[test.results]]
value = "[{ k: 6, n: 2295 }, { k: 6, n: 2288 }, { k: 6, n: 2281 }, { k: 6, n: 2274 }, { k: 6, n: 2267 }, { k: 6, n: 2260 }, { k: 6, n: 2253 }, { k: 6, n: 2246 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3000"

[[test.results]]
value = "0"

[[test.results]]
value = "2999"

[[test.results]]
value = "1"

[[test.results]]
value = "[t:2999, t:2998, t:2997, t:2996, t:2995]"

[[test.results]]
value = "[t:999, t:998, t:997, t:996, t:995]"

[[test.results]]
value = "[{ c: 429, hi: 2996, k: 0 }, { c: 429, hi: 2997, k: 1 }, { c: 429, hi: 2998, k: 2 }, { c: 429, hi: 2999, k: 3 }, { c: 428, hi: 2993, k: 4 }, { c: 428, hi: 2994, k: 5 }, { c: 428, hi: 2995, k: 6 }]"

[[test.results]]
value = "[{ c: 1000, hi: 2997, lo: 0 }]"

[[test.results]]
value = "[{ b: 0, ks: 7 }, { b: 1, ks: 7 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "10"

[[test.results]]
value = "10"

[[test.results]]
value = "NONE"

[[test.results]]
value = "100"

[[test.results]]
value = "100"

[[test.results]]
value = "0"

[[test.results]]
value = "NONE"

[[test.results]]
value = "10"

[[test.results]]
value = "10"
*/

FOR $i IN 0..3000 { CREATE type::record('t', $i) SET n = $i, k = $i % 7, b = $i % 2, s = <string> $i };
LET $p = SELECT n, s FROM t WHERE n % 5 = 1;
array::len($p);
math::sum($p.map(|$r| $r.n));
LET $v = SELECT VALUE n FROM t;
array::len($v);
math::sum($v);
LET $r = SELECT id, n FROM t:500..2500;
array::len($r);
math::sum($r.map(|$x| $x.n));
SELECT n, k FROM t ORDER BY k, n LIMIT 8;
SELECT n, k FROM t ORDER BY k DESC, n DESC LIMIT 8 START 100;
LET $sorted = SELECT VALUE n FROM t ORDER BY b, n;
array::len($sorted);
array::first($sorted);
array::last($sorted);
array::at($sorted, 1500);
SELECT VALUE id FROM t ORDER BY id DESC LIMIT 5;
SELECT VALUE id FROM t ORDER BY id DESC LIMIT 5 START 2000;
SELECT k, count() AS c, math::max(n) AS hi FROM t GROUP BY k ORDER BY k;
SELECT count() AS c, math::min(n) AS lo, math::max(n) AS hi FROM t WHERE n % 3 = 0 GROUP ALL;
SELECT b, array::len(array::group(k)) AS ks FROM t GROUP BY b ORDER BY b;
LET $pg = SELECT VALUE n FROM t LIMIT 10 START 5;
array::len($pg);
array::len(array::distinct($pg));
LET $pw = SELECT VALUE n FROM t WHERE n % 5 = 1 LIMIT 100;
array::len($pw);
array::len(array::distinct($pw));
array::len($pw.filter(|$x| $x % 5 != 1));
LET $tail = SELECT VALUE n FROM t START 2990;
array::len($tail);
array::len(array::distinct($tail));
