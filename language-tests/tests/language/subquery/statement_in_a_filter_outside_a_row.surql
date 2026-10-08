/**
[env]
namespace = true
database = true

[test]
reason = """
A statement inside a `[WHERE ...]` filter is taken over the document root, and
over the element where there is no row: in a `RETURN` or a `LET` value it takes
the element as its `$parent`, in a projection it takes the row. A statement
that reads neither answers the same in both. The same holds for a filter in a
closure body and for `array::filter`.
"""

[[test.results]]
value = "[{ id: p:1, v: 2 }]"

[[test.results]]
value = "[{ id: p:2, v: 4 }]"

[[test.results]]
value = "[{ id: t:1, n: 2, tags: [1, 2, 3] }]"

[[test.results]]
value = "[2, 4]"

[[test.results]]
value = "[{ a: 2 }]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[{ a: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[2, 4]"

[[test.results]]
value = "[[2]]"

[[test.results]]
value = "[[2]]"

[[test.results]]
value = "[[2], []]"

[[test.results]]
value = "[2, 4]"
*/

CREATE p:1 SET v = 2;
CREATE p:2 SET v = 4;
CREATE t:1 SET n = 2, tags = [1, 2, 3];
RETURN [1, 2, 3, 4][WHERE $this IN (SELECT VALUE v FROM p)];
RETURN [{ a: 1 }, { a: 2 }][WHERE a = (SELECT VALUE n FROM ONLY t:1)];
RETURN [1, 2, 3][WHERE (SELECT VALUE $parent FROM ONLY 0) > 1];
RETURN [{ a: 1 }, { a: 3 }][WHERE (SELECT VALUE $parent.a FROM ONLY 0) > 2];
LET $kept = [1, 2, 3, 4][WHERE $this IN (SELECT VALUE v FROM p)];
RETURN $kept;
SELECT VALUE tags[WHERE $this IN (SELECT VALUE v FROM p)] FROM t;
SELECT VALUE tags[WHERE $this = (SELECT VALUE $parent.n FROM ONLY 0)] FROM t;
RETURN [[1, 2], [3]].map(|$a| $a[WHERE $this IN (SELECT VALUE v FROM p)]);
RETURN [1, 2, 3, 4].filter(|$x| $x IN (SELECT VALUE v FROM p));
