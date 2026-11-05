/**
[env]
namespace = true
database = true
imports = ["pgsql/_unqualified_join_columns.surql"]

[test]
reason = "A column named without its table in a join condition is read from the table that holds it, whichever side of the equality it is on, with a key of the other side, a literal, a residual condition or a later join."

[[test.results]]
value = "[{ v: 'a1', w: 'b1' }, { v: 'a2', w: 'b2' }, { v: 'a2', w: NULL }]"

[[test.results]]
value = "[{ v: 'a1', w: 'b1' }, { v: 'a2', w: 'b2' }, { v: 'a2', w: NULL }]"

[[test.results]]
value = "[{ v: 'a1', w: 'b1' }, { v: 'a2', w: 'b2' }, { v: 'a2', w: NULL }]"

[[test.results]]
value = "[{ v: 'a1', w: 'b1' }, { v: 'a2', w: 'b2' }, { v: 'a2', w: NULL }]"

[[test.results]]
value = "[{ v: 'a1', w: 'b1' }, { v: 'a2', w: 'b2' }, { v: 'a2', w: NULL }]"

[[test.results]]
value = "[{ v: 'a1', w: 'b1' }, { v: 'a2', w: 'b2' }, { v: 'a2', w: NULL }]"

[[test.results]]
value = "[{ v: 'a1', w: 'b1' }, { v: 'a2', w: NONE }, { v: 'a3', w: NONE }]"

[[test.results]]
value = "[{ v: 'a1', w: 'b1' }]"

[[test.results]]
value = "[{ v: 'a1', w: 'b1' }]"

[[test.results]]
value = "[{ v: 'a1', w: 'b1', x: 'c1' }]"

[[test.results]]
value = "[{ v: 'a1', w: 'b1', x: 'c1' }]"
*/

SELECT v, w FROM ujc_a JOIN ujc_b ON ak = bk ORDER BY v, w;
SELECT v, w FROM ujc_a JOIN ujc_b ON bk = ak ORDER BY v, w;
SELECT v, w FROM ujc_a JOIN ujc_b ON ujc_a.k = bk ORDER BY v, w;
SELECT v, w FROM ujc_a JOIN ujc_b ON bk = ujc_a.k ORDER BY v, w;
SELECT v, w FROM ujc_a JOIN ujc_b ON ak = ujc_b.k ORDER BY v, w;
SELECT v, w FROM ujc_a JOIN ujc_b ON ujc_b.k = ak ORDER BY v, w;
SELECT v, w FROM ujc_a LEFT JOIN ujc_b ON ak = bk AND w = 'b1' ORDER BY v;
SELECT v, w FROM ujc_a JOIN ujc_b ON ak = 1 AND bk = 1;
SELECT v, w FROM ujc_a JOIN ujc_b ON v = 'a1' AND ujc_a.k = ujc_b.k;
SELECT v, w, x FROM ujc_a JOIN ujc_b ON ak = bk JOIN ujc_c ON ck = ujc_b.k ORDER BY v, w;
SELECT v, w, x FROM ujc_a JOIN ujc_b ON ak = bk JOIN ujc_c ON ck = ak ORDER BY v, w;
