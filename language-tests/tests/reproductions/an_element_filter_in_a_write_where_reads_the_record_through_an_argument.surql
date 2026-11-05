/**
[env]
namespace = true
database = true

[test]
reason = """
An element filter in an UPDATE, DELETE or UPSERT `WHERE` whose body reads an index argument
(`[0, 8, 100][lim]`) or a method argument (`[0, 8, 100].slice(lim).first()`) from the row reads the
record the write judges, over object, scalar and link elements. A closure in that filter reading
the row through such an argument is declined, so only the filter written without a closure is held.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ hit: NONE, id: fu:1 }, { hit: true, id: fu:2 }, { hit: NONE, id: fu:3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: fd:1 }, { id: fd:3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ hit: NONE, id: fl:1 }, { hit: true, id: fl:2 }, { hit: NONE, id: fl:3 }]"
*/
CREATE u:0 SET w = 0 RETURN NONE;
CREATE u:2 SET w = 2 RETURN NONE;
CREATE u:3 SET w = 3 RETURN NONE;
CREATE u:9 SET w = 9 RETURN NONE;
CREATE u:50 SET w = 50 RETURN NONE;
CREATE fu:1 SET lim = 2, os = [{ v: 0 }, { v: 2 }, { v: 9 }, { v: 50 }] RETURN NONE;
CREATE fu:2 SET lim = 0, os = [{ v: 2 }, { v: 9 }] RETURN NONE;
CREATE fu:3 SET lim = 2, os = [{ v: 3 }, { v: 0 }] RETURN NONE;
UPDATE fu SET hit = true WHERE os[WHERE $this.v >= [0, 8, 100][lim]] != [] RETURN NONE;
SELECT id, hit FROM fu ORDER BY id;
CREATE fd:1 SET lim = 2, xs = [0, 2, 9, 50] RETURN NONE;
CREATE fd:2 SET lim = 0, xs = [2, 9] RETURN NONE;
CREATE fd:3 SET lim = 2, xs = [3, 0] RETURN NONE;
DELETE fd WHERE xs[WHERE $this >= [0, 8, 100].slice(lim).first()] != [] RETURN NONE;
SELECT id FROM fd ORDER BY id;
CREATE fl:1 SET lim = 2, ls = [u:0, u:2, u:9, u:50] RETURN NONE;
CREATE fl:2 SET lim = 0, ls = [u:2, u:9] RETURN NONE;
CREATE fl:3 SET lim = 2, ls = [u:3, u:0] RETURN NONE;
UPSERT fl SET hit = true WHERE ls[WHERE $this.w >= [0, 8, 100][lim]] != [] RETURN NONE;
SELECT id, hit FROM fl ORDER BY id;
