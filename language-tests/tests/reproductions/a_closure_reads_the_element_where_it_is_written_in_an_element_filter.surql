/**
[env]
namespace = true
database = true

[test]
reason = "A closure written in an element filter reads the element where it reads the current value, as the closure-free filter does: a field the element does not have is NONE, not the field of the row the filter is judged over. It reads the document root where its body evaluates an index or a method argument, and in a filter over records that root is the record being judged. A closure that is itself a method argument reads, as its current value, the record it is judged over."

[[test.results]]
value = "[{ id: t:1, lim: 8, n: 1 }]"

[[test.results]]
value = "[{ id: t:2, lim: 8, n: 5 }]"

[[test.results]]
value = "[{ id: t:3, lim: 8, n: 10 }]"

[[test.results]]
value = "[{ id: t:4, lim: 8, n: 15 }]"

[[test.results]]
value = "[{ id: t:5, lim: 8, n: 20 }]"

[[test.results]]
value = "[1, 5, 10, 15, 20]"

[[test.results]]
value = "[1, 5, 10, 15, 20]"

[[test.results]]
value = "[{ n: 1, r: [1, 5, 10, 15, 20] }]"

[[test.results]]
value = "[1, 5, 10, 15, 20]"

[[test.results]]
value = "[{ n: 1, r: [1, 5, 10, 15, 20] }]"

[[test.results]]
value = "[{ id: x:1, lim: 1, n: 1 }]"

[[test.results]]
value = "[{ id: x:2, lim: 1, n: 5 }]"

[[test.results]]
value = "[{ id: x:3, lim: 1, n: 10 }]"

[[test.results]]
value = "[{ id: x:4, lim: 1, n: 15 }]"

[[test.results]]
value = "[{ id: x:5, lim: 1, n: 20 }]"

[[test.results]]
value = "[10, 15, 20]"

[[test.results]]
value = "[{ id: y:1, lim: 1, n: 1, o: { lim: 2, v: 10 } }]"

[[test.results]]
value = "[{ id: y:2, lim: 1, n: 5, o: { lim: 2, v: 5 } }]"

[[test.results]]
value = "[{ id: y:3, lim: 1, n: 10, o: { lim: 0, v: 1 } }]"

[[test.results]]
value = "[{ id: yrel:1, in: y:1, lim: 2, out: y:2, w: 10 }]"

[[test.results]]
value = "[{ id: yrel:2, in: y:1, lim: 2, out: y:3, w: 5 }]"

[[test.results]]
value = "[1, 5, 10]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: z:1, r: [0, 2, 9, 50] }, { id: z:2, r: [2, 9] }]"

[[test.results]]
value = "[{ id: z:1, r: [0, 2, 9, 50] }, { id: z:2, r: [2, 9] }]"

[[test.results]]
value = "[{ id: z:1, r: [0, 2, 9, 50] }, { id: z:2, r: [2, 9] }]"

[[test.results]]
value = "[{ id: z:1, r: [0, 2, 9, 50] }, { id: z:2, r: [2, 9] }]"
*/

CREATE t:1 SET n = 1, lim = 8;
CREATE t:2 SET n = 5, lim = 8;
CREATE t:3 SET n = 10, lim = 8;
CREATE t:4 SET n = 15, lim = 8;
CREATE t:5 SET n = 20, lim = 8;
SELECT VALUE n FROM t WHERE [n][WHERE array::any([$this], |$e| $e >= lim)] != [] ORDER BY n;
SELECT VALUE n FROM t WHERE [n][WHERE array::any([$this], |$e| array::any([$e], |$q| $q >= lim))] != [] ORDER BY n;
SELECT n, [1, 5, 10, 15, 20][WHERE array::any([$this], |$q| $q >= lim)] AS r FROM t:1;
SELECT VALUE n FROM t WHERE [n][WHERE $this >= lim] != [] ORDER BY n;
SELECT n, [1, 5, 10, 15, 20][WHERE $this >= lim] AS r FROM t:1;
CREATE x:1 SET n = 1, lim = 1;
CREATE x:2 SET n = 5, lim = 1;
CREATE x:3 SET n = 10, lim = 1;
CREATE x:4 SET n = 15, lim = 1;
CREATE x:5 SET n = 20, lim = 1;
SELECT VALUE n FROM x WHERE [n][WHERE array::any([$this], |$e| $e >= [0, 8, 100][lim])] != [] ORDER BY n;
CREATE y:1 SET n = 1, lim = 1, o = { lim: 2, v: 10 };
CREATE y:2 SET n = 5, lim = 1, o = { lim: 2, v: 5 };
CREATE y:3 SET n = 10, lim = 1, o = { lim: 0, v: 1 };
RELATE y:1->yrel:1->y:2 SET w = 10, lim = 2;
RELATE y:1->yrel:2->y:3 SET w = 5, lim = 2;
SELECT VALUE n FROM y WHERE [{ a: [1, 10] }][WHERE a.filter(|$e| $e >= lim + 4) != []] != [] ORDER BY n;
SELECT VALUE n FROM y WHERE [o][WHERE array::any([1], |$e| v >= [0, 8, 100][lim])] != [] ORDER BY n;
SELECT VALUE n FROM y WHERE ->yrel[WHERE array::any([1], |$e| w >= [0, 8, 100][lim])] != [] ORDER BY n;
SELECT VALUE n FROM y WHERE [o][WHERE [1].map(|$e| $this.v)[0] >= 8] != [] ORDER BY n;
SELECT VALUE n FROM y WHERE [o][WHERE [1].map(|$e| v)[0] >= 8] != [] ORDER BY n;
SELECT VALUE n FROM y WHERE [o][WHERE array::any([1], |$e| $this.v >= [0, 8, 100][lim])] != [] ORDER BY n;
SELECT VALUE n FROM y WHERE [o][WHERE (|$e| v >= [0, 8, 100][lim])(1)] != [] ORDER BY n;
CREATE z:1 SET lim = 2, xs = [0, 2, 9, 50] RETURN NONE;
CREATE z:2 SET lim = 0, xs = [2, 9] RETURN NONE;
SELECT id, xs[WHERE array::any([$this], |$e| $e >= $this.lim)] AS r FROM z ORDER BY id;
SELECT id, xs[WHERE array::any([$this], |$e| $e >= lim)] AS r FROM z ORDER BY id;
SELECT id, xs[WHERE array::any([$this], |$e| $e >= type::field('lim'))] AS r FROM z ORDER BY id;
SELECT id, xs[WHERE $this >= lim] AS r FROM z ORDER BY id;
