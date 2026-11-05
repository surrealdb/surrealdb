/**
[env]
namespace = true
database = true

[test]
reason = "A filter over records judges each record with the record as the document root: an index or a method argument in an element body of its predicate reads the record, as it does in a projection, so the predicate runs whatever the row source is (a scan, an id range, a single record, a union, an edge table, a dynamic table, an index lookup, a split) and whatever wraps it (a group, a limit, a fetch, a subquery). The closure-free element filter that reads the element, not the record, is the control. A closure in an element filter of an UPDATE `SET` clause reads the record's index argument as the closure-free filter in that clause does."

[[test.results]]
value = "[{ id: t:1, k: 9, lim: 1, n: 1, o: { lim: 2, v: 10 }, xs: [1, 2] }]"

[[test.results]]
value = "[{ id: t:2, k: 1, lim: 1, n: 5, o: { lim: 2, v: 5 }, xs: [3] }]"

[[test.results]]
value = "[{ id: t:3, k: 9, lim: 1, n: 10, o: { lim: 0, v: 1 }, xs: [4] }]"

[[test.results]]
value = "[{ id: rel:1, in: t:1, lim: 2, n: 7, out: t:2, w: 10 }]"

[[test.results]]
value = "[{ id: rel:2, in: t:1, lim: 2, n: 7, out: t:3, w: 5 }]"

[[test.results]]
value = "[{ id: rel2:1, in: t:2, lim: 1, out: t:3, w: 10 }]"

[[test.results]]
value = "[{ id: rel2:2, in: t:2, lim: 1, out: t:1, w: 5 }]"

[[test.results]]
value = "[10]"

[[test.results]]
value = "[10]"

[[test.results]]
value = "[{ s: 10 }]"

[[test.results]]
value = "[10]"

[[test.results]]
value = "10"

[[test.results]]
value = "[]"

[[test.results]]
value = "[10]"

[[test.results]]
value = "[{ lim: 1, n: 10 }]"

[[test.results]]
value = "[10]"

[[test.results]]
value = "[10]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10]"

[[test.results]]
value = "[10]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[{ xs: 4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[5, 10]"

[[test.results]]
value = "[10]"

[[test.results]]
value = "[10]"

[[test.results]]
value = "[1, 5, 10]"

[[test.results]]
value = "[[1], [5], [10]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10]"

[[test.results]]
value = "[10]"

[[test.results]]
value = "[{ id: r1:1, lim: 1, n: 10 }]"

[[test.results]]
value = "[{ id: r1:2, lim: 1, n: 5 }]"

[[test.results]]
value = "[{ id: r1:3, lim: 2, n: 1 }]"

[[test.results]]
value = "[[10], [], []]"

[[test.results]]
value = "[[10], [], []]"

*/

CREATE t:1 SET n = 1, lim = 1, k = 9, xs = [1, 2], o = { lim: 2, v: 10 };
CREATE t:2 SET n = 5, lim = 1, k = 1, xs = [3], o = { lim: 2, v: 5 };
CREATE t:3 SET n = 10, lim = 1, k = 9, xs = [4], o = { lim: 0, v: 1 };
RELATE t:1->rel:1->t:2 SET w = 10, lim = 2, n = 7;
RELATE t:1->rel:2->t:3 SET w = 5, lim = 2, n = 7;
RELATE t:2->rel2:1->t:3 SET w = 10, lim = 1;
RELATE t:2->rel2:2->t:1 SET w = 5, lim = 1;
SELECT VALUE n FROM t WHERE [n][WHERE $this >= [0, 8, 100][lim]] != [] ORDER BY n;
SELECT VALUE n FROM t WHERE [n][WHERE $this >= [0, 8, 100].at(lim)] != [] ORDER BY n;
SELECT math::sum(n) AS s FROM t WHERE [n][WHERE $this >= [0, 8, 100][lim]] != [] GROUP ALL;
SELECT VALUE n FROM t:1..=3 WHERE [n][WHERE $this >= [0, 8, 100][lim]] != [];
SELECT VALUE n FROM ONLY t:3 WHERE [n][WHERE $this >= [0, 8, 100][lim]] != [];
SELECT VALUE n FROM t:2 WHERE [n][WHERE $this >= [0, 8, 100][lim]] != [];
SELECT VALUE n FROM t WHERE [n][WHERE $this >= [0, 8, 100][lim]] != [] ORDER BY n LIMIT 1;
SELECT n, lim FROM t WHERE [n][WHERE $this >= [0, 8, 100][lim]] != [] ORDER BY n FETCH lim;
SELECT VALUE n FROM t WHERE [n][WHERE $this >= [0, 8, 100][$parent.lim]] != [] ORDER BY n;
RETURN (SELECT VALUE n FROM t WHERE [n][WHERE $this >= [0, 8, 100][lim]] != [] ORDER BY n);
LET $tb = 't';
SELECT VALUE n FROM type::table($tb) WHERE [n][WHERE $this >= [0, 8, 100][lim]] != [] ORDER BY n;
SELECT VALUE w FROM rel2 WHERE [w][WHERE $this >= [0, 8, 100][lim]] != [] ORDER BY w;
SELECT VALUE id FROM t WHERE ->rel[WHERE [w][WHERE $this >= [0, 8, 100][lim]] != []] != [] ORDER BY id;
SELECT xs FROM t WHERE [n][WHERE $this >= [0, 8, 100][lim]] != [] SPLIT xs;
SELECT VALUE n FROM t WHERE [0, 1, 2][WHERE [10, 20, 30][$this] = 20] != [] ORDER BY n;
SELECT VALUE n FROM t WHERE [o][WHERE v >= [0, 8, 100][@.lim]] != [] ORDER BY n;
SELECT VALUE n FROM t WHERE [n][WHERE $this >= [0, 8, 100][count(->rel)]] != [] ORDER BY n;
SELECT VALUE n FROM t:1, t:3 WHERE [n][WHERE $this >= [0, 8, 100][lim]] != [] ORDER BY n;
SELECT VALUE n FROM t, rel WHERE [n][WHERE $this >= [0, 8, 100][lim]] != [] ORDER BY n;
SELECT VALUE n FROM t WHERE [n][WHERE $this >= lim] != [] ORDER BY n;
SELECT VALUE [n][WHERE $this >= lim] FROM t ORDER BY n;
DEFINE FIELD c ON t COMPUTED n;
SELECT VALUE n FROM t WHERE [c][WHERE $this >= [0, 8, 100][lim]] != [] ORDER BY n;
DEFINE INDEX ix ON t FIELDS n;
SELECT VALUE n FROM t WHERE n >= 5 AND [n][WHERE $this >= [0, 8, 100][lim]] != [] ORDER BY n;
SELECT VALUE n FROM t WHERE n = 10 AND [n][WHERE $this >= [0, 8, 100][lim]] != [];
CREATE r1:1 SET n = 10, lim = 1;
CREATE r1:2 SET n = 5, lim = 1;
CREATE r1:3 SET n = 1, lim = 2;
UPDATE r1 SET g = [n][WHERE array::any([$this], |$e| $e >= [0, 8, 100][lim])] RETURN VALUE g;
UPDATE r1 SET g = [n][WHERE $this >= [0, 8, 100][lim]] RETURN VALUE g;
