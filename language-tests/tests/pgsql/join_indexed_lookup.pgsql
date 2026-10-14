/**
[env]
namespace = true
database = true
imports = ["pgsql/_join_lookup.surql"]

[test]
reason = "A join whose right input is a table indexed on the join key, for fewer left rows than the lookup is read for, as many as the lookup switches to the build table at, and more. The first rows of the first queries carry keys an index may not list (NONE, NULL, an array), which read the build table from the first row; the queries that start at n = -2 carry keys it lists, so the lookup serves their first rows, across numeric types, and hands over to the build table. The pairs and their order are those the build table gives."

[[test.results]]
value = "[{ n: -5, s: 1005 }, { n: -4, s: 1004 }, { n: -3, s: 1006 }, { n: -2, s: 1 }, { n: -2, s: 1001 }, { n: -2, s: 1002 }, { n: -1, s: 1 }, { n: -1, s: 1001 }, { n: -1, s: 1002 }, { n: 0, s: 0 }, { n: 1, s: 2 }, { n: 2, s: 4 }]"

[[test.results]]
value = "[{ n: -5, s: 1005 }, { n: -4, s: 1004 }, { n: -3, s: 1006 }, { n: -2, s: 1 }, { n: -2, s: 1001 }, { n: -2, s: 1002 }, { n: -1, s: 1 }, { n: -1, s: 1001 }, { n: -1, s: 1002 }, { n: 0, s: 0 }, { n: 1, s: 2 }, { n: 2, s: 4 }]"

[[test.results]]
value = "[{ c: 54, t: 9003 }]"

[[test.results]]
value = "[{ c: 74, t: 11183 }]"

[[test.results]]
value = "[{ c: 84, t: 12573 }]"

[[test.results]]
value = "[{ c: 129, matched: 84 }]"

[[test.results]]
value = "[{ n: -5, s: 1005 }, { n: -4, s: 1004 }, { n: -3, s: 1006 }, { n: -2, s: 1001 }, { n: -2, s: 1002 }, { n: -1, s: 1001 }, { n: -1, s: 1002 }, { n: 31, s: 62 }, { n: 32, s: 64 }, { n: 33, s: 66 }, { n: 34, s: 68 }, { n: 35, s: 70 }, { n: 36, s: 72 }, { n: 37, s: 74 }, { n: 38, s: 76 }, { n: 39, s: 78 }, { n: 40, s: 80 }, { n: 41, s: 82 }, { n: 42, s: 84 }, { n: 43, s: 86 }, { n: 44, s: 88 }, { n: 45, s: 90 }, { n: 46, s: 92 }, { n: 47, s: 94 }, { n: 48, s: 96 }, { n: 49, s: 98 }, { n: 50, s: 100 }, { n: 51, s: 102 }, { n: 52, s: 104 }, { n: 53, s: 106 }, { n: 54, s: 108 }, { n: 55, s: 110 }, { n: 56, s: 112 }, { n: 57, s: 114 }, { n: 58, s: 116 }, { n: 59, s: 118 }, { n: 60, s: 120 }, { n: 61, s: 122 }, { n: 62, s: 124 }, { n: 63, s: 126 }, { n: 64, s: 128 }]"

[[test.results]]
value = "[{ n: 0, s: 0 }]"

[[test.results]]
value = "[{ c: 76, t: 12541 }]"

[[test.results]]
value = "[{ c: 76, t: 12541 }]"

[[test.results]]
value = "[{ c: 0 }]"

[[test.results]]
value = "[{ n: -2, s: 1 }, { n: -2, s: 1001 }, { n: -2, s: 1002 }, { n: -1, s: 1 }, { n: -1, s: 1001 }, { n: -1, s: 1002 }, { n: 0, s: 0 }, { n: 1, s: 2 }, { n: 2, s: 4 }]"

[[test.results]]
value = "[{ n: -2, s: 1 }, { n: -2, s: 1001 }, { n: -2, s: 1002 }, { n: -1, s: 1 }, { n: -1, s: 1001 }, { n: -1, s: 1002 }, { n: 0, s: 0 }, { n: 1, s: 2 }, { n: 2, s: 4 }]"

[[test.results]]
value = "[{ c: 74, t: 8564 }]"

[[test.results]]
value = "[{ c: 81, t: 9558 }]"

[[test.results]]
value = "[{ c: 126, matched: 81 }]"

[[test.results]]
value = "[{ n: 59, s: 118 }, { n: 60, s: 120 }, { n: 61, s: 122 }, { n: 62, s: 124 }, { n: 63, s: 126 }, { n: 64, s: 128 }, { n: 65, s: 130 }, { n: 66, s: 132 }, { n: 67, s: 134 }]"
*/

SELECT l.n, r.s FROM (SELECT n, k FROM a ORDER BY n LIMIT 8) l JOIN b r ON l.k = r.k ORDER BY l.n, r.s;

SELECT l.n, r.s FROM (SELECT n, k FROM a ORDER BY n LIMIT 8) l LEFT JOIN b r ON l.k = r.k ORDER BY l.n, r.s;

SELECT count(*) AS c, sum(r.s) AS t FROM (SELECT n, k FROM a ORDER BY n LIMIT 50) l JOIN b r ON l.k = r.k;

SELECT count(*) AS c, sum(r.s) AS t FROM (SELECT n, k FROM a ORDER BY n LIMIT 70) l JOIN b r ON l.k = r.k;

SELECT count(*) AS c, sum(r.s) AS t FROM (SELECT n, k FROM a ORDER BY n LIMIT 125) l JOIN b r ON l.k = r.k;

SELECT count(*) AS c, count(r.s) AS matched FROM (SELECT n, k FROM a ORDER BY n LIMIT 125) l LEFT JOIN b r ON l.k = r.k;

SELECT l.n, r.s FROM (SELECT n, k FROM a ORDER BY n LIMIT 70) l JOIN b r ON l.k = r.k AND r.s > 60 ORDER BY l.n, r.s;

SELECT l.n, r.s FROM (SELECT n, k FROM a ORDER BY n LIMIT 70) l JOIN b r ON l.n = r.s AND l.k = r.k ORDER BY l.n, r.s;

SELECT count(*) AS c, sum(r.s) AS t FROM (SELECT n, k FROM a ORDER BY n LIMIT 80) l JOIN (SELECT * FROM b WHERE s > 10) r ON l.k = r.k;

SELECT count(*) AS c, sum(r.s) AS t FROM (SELECT n, k FROM a ORDER BY n LIMIT 80) l JOIN (SELECT k, s FROM b WHERE s > 10) r ON l.k = r.k;

SELECT count(*) AS c FROM (SELECT n, k FROM a ORDER BY n LIMIT 0) l JOIN b r ON l.k = r.k;

SELECT l.n, r.s FROM (SELECT n, k FROM a WHERE n >= -2 ORDER BY n LIMIT 5) l JOIN b r ON l.k = r.k ORDER BY l.n, r.s;

SELECT l.n, r.s FROM (SELECT n, k FROM a WHERE n >= -2 ORDER BY n LIMIT 5) l LEFT JOIN b r ON l.k = r.k ORDER BY l.n, r.s;

SELECT count(*) AS c, sum(r.s) AS t FROM (SELECT n, k FROM a WHERE n >= -2 ORDER BY n LIMIT 70) l JOIN b r ON l.k = r.k;

SELECT count(*) AS c, sum(r.s) AS t FROM (SELECT n, k FROM a WHERE n >= -2 ORDER BY n LIMIT 125) l JOIN b r ON l.k = r.k;

SELECT count(*) AS c, count(r.s) AS matched FROM (SELECT n, k FROM a WHERE n >= -2 ORDER BY n LIMIT 125) l LEFT JOIN b r ON l.k = r.k;

SELECT l.n, r.s FROM (SELECT n, k FROM a WHERE n >= -2 ORDER BY n LIMIT 70) l JOIN b r ON l.k = r.k WHERE l.n > 58 AND l.n < 68 ORDER BY l.n, r.s;
