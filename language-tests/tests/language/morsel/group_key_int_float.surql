/**
[env]
namespace = true
database = true
timeout = 20000

[test]
reason = "keys that are equal and differ in type, 1 and 1.0, share a group, which keeps the key of the first row the scan reads"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 3000, k: 1, top: 2999 }]"

[[test.results]]
value = "[{ c: 3000, k: 1f, top: 2999 }]"

[[test.results]]
value = "[{ k: 1, ns: [0, 1, 2, 3, 4, 5] }]"

[[test.results]]
value = "[{ c: 3000 }]"
*/

FOR $i IN 0..3000 { CREATE type::record('ints', $i) SET k = IF $i % 2 = 0 { 1 } ELSE { 1.0f }, n = $i };
FOR $i IN 0..3000 { CREATE type::record('floats', $i) SET k = IF $i % 2 = 0 { 1.0f } ELSE { 1 }, n = $i };
SELECT k, count() AS c, math::max(n) AS top FROM ints GROUP BY k;
SELECT k, count() AS c, math::max(n) AS top FROM floats GROUP BY k;
SELECT k, array::group(n) AS ns FROM ints WHERE n < 6 GROUP BY k;
SELECT count() AS c FROM ints GROUP ALL;
