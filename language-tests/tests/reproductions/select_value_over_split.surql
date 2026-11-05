/**
[env]
namespace = true
database = true

[test]
reason = """
A `SELECT VALUE <split field> ... SPLIT <split field>` splits the record on the field and returns
each element, as `SELECT <split field> ... SPLIT <split field>` returns each element in an object.
The split happens once: one row per element, the element itself as the value. The same holds with
ORDER BY, LIMIT, START, WHERE before SPLIT, FETCH, and with a nested path or an array of objects as
the split field. `SELECT * OMIT ... SPLIT` keeps the omitted field out of every split row.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "['x', 'y', 'z']"

[[test.results]]
value = "['z', 'y', 'x']"

[[test.results]]
value = "['x', 'y']"

[[test.results]]
value = "['z']"

[[test.results]]
value = "['y']"

[[test.results]]
value = "['x', 'y']"

[[test.results]]
value = "['x', 'y', 'z']"

[[test.results]]
value = "[1, 2, 5]"

[[test.results]]
value = "[5, 2, 1]"

[[test.results]]
value = "[{ a: 1, t: 'p' }, { a: 2, t: 'q' }, { a: 3, t: 'r' }]"

[[test.results]]
value = "[{ a: 3, t: 'r' }]"

[[test.results]]
value = "[{ id: sp:1, m: [3, 4], n: { b: { c: [1, 2] } }, o: [{ a: 1, t: 'p' }, { a: 2, t: 'q' }], t: 'x' }, { id: sp:1, m: [3, 4], n: { b: { c: [1, 2] } }, o: [{ a: 1, t: 'p' }, { a: 2, t: 'q' }], t: 'y' }, { id: sp:2, m: [6, 7], n: { b: { c: [5] } }, o: [{ a: 3, t: 'r' }], t: 'z' }]"

[[test.results]]
value = "[{ k: 1, t: 'x' }, { k: 1, t: 'y' }, { k: 2, t: 'z' }]"

[[test.results]]
value = "[{ t: 'x' }, { t: 'y' }, { t: 'z' }]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[2, 1]"

[[test.results]]
value = "[1, 2]"

*/

DEFINE TABLE sp;
CREATE sp:1 SET t = ['x', 'y'], k = 1, o = [{ a: 1, t: 'p' }, { a: 2, t: 'q' }], n = { b: { c: [1, 2] } }, m = [3, 4] RETURN NONE;
CREATE sp:2 SET t = ['z'], k = 2, o = [{ a: 3, t: 'r' }], n = { b: { c: [5] } }, m = [6, 7] RETURN NONE;
SELECT VALUE t FROM sp SPLIT t;
SELECT VALUE t FROM sp SPLIT t ORDER BY t DESC;
SELECT VALUE t FROM sp SPLIT t LIMIT 2;
SELECT VALUE t FROM sp SPLIT t ORDER BY t DESC LIMIT 1;
SELECT VALUE t FROM sp SPLIT t LIMIT 1 START 1;
SELECT VALUE t FROM sp WHERE k = 1 SPLIT t;
SELECT VALUE t FROM sp SPLIT t FETCH t;
SELECT VALUE n.b.c FROM sp SPLIT n.b.c;
SELECT VALUE n.b.c FROM sp SPLIT n.b.c ORDER BY n.b.c DESC;
SELECT VALUE o FROM sp SPLIT o;
SELECT VALUE o FROM sp SPLIT o ORDER BY o.a DESC LIMIT 1;
SELECT * OMIT k FROM sp SPLIT t;
SELECT k, t FROM sp SPLIT t ORDER BY t;
SELECT t FROM sp SPLIT t;
SELECT VALUE x FROM [{ x: 2 }, { x: 1 }] SPLIT x ORDER BY x;
SELECT VALUE x FROM [{ x: 2 }, { x: 1 }] SPLIT x ORDER BY x DESC;
SELECT VALUE x AS y FROM [{ x: 2 }, { x: 1 }] SPLIT y ORDER BY y;
