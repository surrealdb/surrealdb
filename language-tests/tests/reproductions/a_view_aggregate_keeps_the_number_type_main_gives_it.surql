/**
[env]
namespace = true
database = true

[test]
reason = "A mean a view keeps over integers is a float, whether the view is filled from rows that exist when it is defined or maintained as rows are written; a sum over integers is an exact integer, as the ad hoc sum is; over decimals both are decimals. The types are pinned with type::is_*, since the numeric expectations below are type-blind."

[[test.results]]
value = "[{ g: 'a', id: e:1, v: 1 }]"

[[test.results]]
value = "[{ g: 'a', id: e:2, v: 2 }]"

[[test.results]]
value = "[{ g: 'b', id: e:3, v: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ avg: 2f, c: 3, id: m:[], mn: 1, mx: 3, s: 6f }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ avg: 1.5f, c: 2, g: 'a', id: m2:['a'], mn: 1, mx: 2, s: 3f }, { avg: 3f, c: 1, g: 'b', id: m2:['b'], mn: 3, mx: 3, s: 3f }]"

[[test.results]]
value = "[{ g: 'b', id: e:4, v: 4 }]"

[[test.results]]
value = "[{ avg: 2.5f, c: 4, id: m:[], mn: 1, mx: 4, s: 10f }]"

[[test.results]]
value = "[{ avg: 1.5f, c: 2, g: 'a', id: m2:['a'], mn: 1, mx: 2, s: 3f }, { avg: 3.5f, c: 2, g: 'b', id: m2:['b'], mn: 3, mx: 4, s: 7f }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ avg: 3f, c: 3, id: m:[], mn: 2, mx: 4, s: 9f }]"

[[test.results]]
value = "[{ avg: 2f, c: 1, g: 'a', id: m2:['a'], mn: 2, mx: 2, s: 2f }, { avg: 3.5f, c: 2, g: 'b', id: m2:['b'], mn: 3, mx: 4, s: 7f }]"

[[test.results]]
value = "[{ g: 'a', id: e:2, v: 10 }]"

[[test.results]]
value = "[{ avg: 5.666666666666667f, c: 3, id: m:[], mn: 3, mx: 10, s: 17f }]"

[[test.results]]
value = "[{ avg: 10f, c: 1, g: 'a', id: m2:['a'], mn: 10, mx: 10, s: 10f }, { avg: 3.5f, c: 2, g: 'b', id: m2:['b'], mn: 3, mx: 4, s: 7f }]"

[[test.results]]
value = "[{ id: f:1, v: 1.5f }]"

[[test.results]]
value = "[{ id: f:2, v: 2dec }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ avg: 1.750dec, id: m3:[], s: 3.5dec }]"

[[test.results]]
value = "[{ avg: 5.666666666666667f, s: 17 }]"

[[test.results]]
value = "[{ avg_float: true, mn_int: true, s_int: true }]"

[[test.results]]
value = "[{ g: 'a', id: e:2, v: 11 }]"

[[test.results]]
value = "[{ avg_float: true, mn_int: true, s_int: true }]"

*/
CREATE e:1 SET v = 1, g = 'a';
CREATE e:2 SET v = 2, g = 'a';
CREATE e:3 SET v = 3, g = 'b';
DEFINE TABLE m AS SELECT math::mean(v) AS avg, math::sum(v) AS s, math::min(v) AS mn, math::max(v) AS mx, count() AS c FROM e GROUP ALL;
SELECT * FROM m;
DEFINE TABLE m2 AS SELECT g, math::mean(v) AS avg, math::sum(v) AS s, math::min(v) AS mn, math::max(v) AS mx, count() AS c FROM e GROUP BY g;
SELECT * FROM m2 ORDER BY g;
CREATE e:4 SET v = 4, g = 'b';
SELECT * FROM m;
SELECT * FROM m2 ORDER BY g;
DELETE e:1;
SELECT * FROM m;
SELECT * FROM m2 ORDER BY g;
UPDATE e:2 SET v = 10;
SELECT * FROM m;
SELECT * FROM m2 ORDER BY g;
CREATE f:1 SET v = 1.5;
CREATE f:2 SET v = 2dec;
DEFINE TABLE m3 AS SELECT math::mean(v) AS avg, math::sum(v) AS s FROM f GROUP ALL;
SELECT * FROM m3;
SELECT math::mean(v) AS avg, math::sum(v) AS s FROM e GROUP ALL;
SELECT type::is_float(avg) AS avg_float, type::is_int(s) AS s_int, type::is_int(mn) AS mn_int FROM m;
UPDATE e:2 SET v = 11;
SELECT type::is_float(avg) AS avg_float, type::is_int(s) AS s_int, type::is_int(mn) AS mn_int FROM m;
