/**
[env]
namespace = true
database = true
imports = ["pgsql/_unqualified_join_columns.surql"]

[test]
reason = "A column named without its table in a join is read from whichever table holds it: in the select list, WHERE, ORDER BY, GROUP BY and HAVING, and through aggregates, for inner, left and right joins, comma joins and chains of three tables."

[[test.results]]
value = "[{ v: 'a1', w: 'b1' }, { v: 'a2', w: 'b2' }, { v: 'a2', w: NULL }]"

[[test.results]]
value = "[{ v: 'a1', w: 'b1' }, { v: 'a2', w: 'b2' }, { v: 'a2', w: NULL }, { v: 'a3', w: NONE }]"

[[test.results]]
value = "[{ v: 'a1', w: 'b1' }, { v: 'a2', w: 'b2' }, { v: NONE, w: 'b4' }]"

[[test.results]]
value = "[{ v: 'a2', w: 'b2' }]"

[[test.results]]
value = "[{ v: 'a2' }, { v: 'a3' }]"

[[test.results]]
value = "[{ v: 'a1', w: 'b1' }]"

[[test.results]]
value = "[{ v: 'a1', w: 'b1', x: 'c1' }]"

[[test.results]]
value = "[{ v: 'a1', w: 'b1', x: 'c1' }, { v: 'a2', w: 'b2', x: NONE }, { v: 'a3', w: NONE, x: NONE }]"

[[test.results]]
value = "[{ v: 'a2', w: 'b2' }, { v: 'a1', w: 'b1' }]"

[[test.results]]
value = "[{ v: 'a1', w: 'b1' }, { v: 'a2', w: 'b2' }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 1, v: 'a1' }, { count: 1, v: 'a2' }, { count: 0, v: 'a3' }]"

[[test.results]]
value = "[{ count: 1, w: 'b1' }, { count: 1, w: 'b2' }]"

[[test.results]]
value = "[{ max: 'b2', v: 'a2' }]"

[[test.results]]
value = "[{ count: 1, v: 'a1' }, { count: 2, v: 'a2' }]"

[[test.results]]
value = "[{ count: 1, v: 'a1' }, { count: 2, v: 'a2' }]"

[[test.results]]
value = "[{ count: 1, w: 'b1' }, { count: 1, w: 'b2' }, { count: 1, w: NULL }]"

[[test.results]]
value = "[{ count: 1, w: 'b1' }, { count: 1, w: 'b2' }]"

[[test.results]]
value = "[{ count: 1, upper: 'A1' }, { count: 2, upper: 'A2' }]"

[[test.results]]
value = "[{ count: 1, upper: 'A1' }, { count: 2, upper: 'A2' }]"

[[test.results]]
value = "[{ sum: 1 }, { sum: 4 }]"
*/

SELECT v, w FROM ujc_a JOIN ujc_b ON ujc_a.k = ujc_b.k ORDER BY v, w;
SELECT v, w FROM ujc_a LEFT JOIN ujc_b ON ujc_a.k = ujc_b.k ORDER BY v, w;
SELECT v, w FROM ujc_a RIGHT JOIN ujc_b ON ujc_a.k = ujc_b.k WHERE w IS NOT NULL ORDER BY w;
SELECT v, w FROM ujc_a JOIN ujc_b ON ujc_a.k = ujc_b.k WHERE w = 'b2';
SELECT v FROM ujc_a LEFT JOIN ujc_b ON ujc_a.k = ujc_b.k WHERE w IS NULL ORDER BY v;
SELECT v, w FROM ujc_a, ujc_b WHERE ujc_a.k = ujc_b.k AND w = 'b1';
SELECT v, w, x FROM ujc_a JOIN ujc_b ON ujc_a.k = ujc_b.k JOIN ujc_c ON ujc_b.k = ujc_c.k;
SELECT v, w, x FROM ujc_a LEFT JOIN ujc_b ON ujc_a.k = ujc_b.k AND w IS NOT NULL LEFT JOIN ujc_c ON ujc_a.k = ujc_c.k ORDER BY v;
SELECT v, w FROM ujc_a JOIN ujc_b ON ujc_a.k = ujc_b.k WHERE w IS NOT NULL ORDER BY w DESC;
SELECT v, w FROM ujc_a JOIN ujc_b ON ujc_a.k = ujc_b.k WHERE w LIKE 'b%' AND w IN ('b1', 'b2') ORDER BY v;
SELECT count(w) FROM ujc_a LEFT JOIN ujc_b ON ujc_a.k = ujc_b.k;
SELECT v, count(w) FROM ujc_a LEFT JOIN ujc_b ON ujc_a.k = ujc_b.k GROUP BY v ORDER BY v;
SELECT w, count(*) FROM ujc_a JOIN ujc_b ON ujc_a.k = ujc_b.k WHERE w IS NOT NULL GROUP BY w ORDER BY w;
SELECT v, max(w) FROM ujc_a LEFT JOIN ujc_b ON ujc_a.k = ujc_b.k GROUP BY v HAVING max(w) > 'b1' ORDER BY v;
SELECT v, count(*) FROM ujc_a JOIN ujc_b ON ujc_a.k = ujc_b.k GROUP BY ujc_a.v ORDER BY v;
SELECT ujc_a.v, count(*) FROM ujc_a JOIN ujc_b ON ujc_a.k = ujc_b.k GROUP BY v ORDER BY 1;
SELECT w, count(*) FROM ujc_a JOIN ujc_b ON ujc_a.k = ujc_b.k GROUP BY ujc_b.w ORDER BY w;
SELECT ujc_b.w, count(*) FROM ujc_a JOIN ujc_b ON ujc_a.k = ujc_b.k WHERE w IS NOT NULL GROUP BY w ORDER BY 1;
SELECT upper(v), count(*) FROM ujc_a JOIN ujc_b ON ujc_a.k = ujc_b.k GROUP BY upper(v) ORDER BY 1;
SELECT upper(v), count(*) FROM ujc_a JOIN ujc_b ON ujc_a.k = ujc_b.k GROUP BY upper(ujc_a.v) ORDER BY 1;
SELECT sum(ak) FROM ujc_a JOIN ujc_b ON ujc_a.k = ujc_b.k GROUP BY v ORDER BY 1;
