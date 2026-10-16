/**
[env]
namespace = true
database = true

[test]
reason = """
	A subquery read only for existence is cut to its first row only where no row it would not read
	could raise: a cast that rejects the second row of the subquery, a function that raises, a
	nested statement, or a step through a record that fetches, all keep the whole read, and the
	statement raises the error the whole read raises. An outer row that is no document keeps a
	correlated read whole too.
"""

[[test.results]]
value = "[{ id: outer:1, n: 1, x: 1 }]"

[[test.results]]
value = "[{ id: outer:2, n: 2, x: 2 }]"

[[test.results]]
value = "[{ id: box:1, s: '1', x: 1 }]"

[[test.results]]
value = "[{ id: box:2, s: 'bad', x: 1 }]"

[[test.results]]
value = "[{ id: box:3, s: '3', x: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Could not cast into `int` using input `'bad'`"

[[test.results]]
error = "Could not cast into `int` using input `'bad'`"

[[test.results]]
error = "Could not cast into `int` using input `'bad'`"

[[test.results]]
error = "Could not cast into `int` using input `'bad'`"

[[test.results]]
error = "Could not cast into `int` using input `'bad'`"

[[test.results]]
value = "[outer:1, outer:2]"

[[test.results]]
value = "[outer:1, outer:2]"

[[test.results]]
error = "An error occurred: boom"

[[test.results]]
error = "An error occurred: boom"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: link:1, x: 1 }]"

[[test.results]]
value = "[outer:1, outer:2]"

[[test.results]]
value = "[outer:1, outer:2]"

[[test.results]]
value = "[link:1]"
*/

CREATE outer:1 SET x = 1, n = 1;
CREATE outer:2 SET x = 2, n = 2;
CREATE box:1 SET s = '1', x = 1;
CREATE box:2 SET s = 'bad', x = 1;
CREATE box:3 SET s = '3', x = 2;
DEFINE FUNCTION fn::raise_on($x: int) { IF $x = 2 { THROW 'boom'; }; RETURN true; };
-- A cast that rejects the second row, in the projection and in the WHERE.
SELECT VALUE id FROM outer WHERE (SELECT <int> s FROM box);
SELECT VALUE id FROM outer WHERE (SELECT id FROM box WHERE <int> s > 0);
SELECT VALUE id FROM outer WHERE (SELECT <int> s FROM box WHERE x = $parent.x);
SELECT VALUE id FROM outer WHERE !(SELECT id FROM box WHERE <int> s > 0);
SELECT VALUE (SELECT <int> s FROM box) FROM outer:1;
-- The same casts that raise on no row.
SELECT VALUE id FROM outer WHERE (SELECT <int> s FROM box WHERE x = 2);
SELECT VALUE id FROM outer WHERE (SELECT id FROM box WHERE x = 2 AND <int> s > 0);
-- A function that raises on the second row.
SELECT VALUE id FROM outer WHERE (SELECT id FROM box WHERE fn::raise_on(x));
SELECT VALUE id FROM outer WHERE (SELECT id FROM box WHERE x = $parent.x AND fn::raise_on(x));
-- A nested statement, and a step through a record link.
SELECT VALUE id FROM outer WHERE (SELECT id FROM box WHERE (SELECT id FROM outer WHERE x = 99));
SELECT VALUE id FROM outer WHERE (SELECT id FROM box WHERE x = $parent.x.y);
-- An outer row that is a record id, or a range of them: a step into it fetches.
CREATE link:1 SET x = 1;
SELECT VALUE id FROM [outer:1, outer:2] WHERE (SELECT id FROM box WHERE x = $parent.x);
SELECT VALUE id FROM outer:1..3 WHERE (SELECT id FROM box WHERE x = $parent.x);
SELECT VALUE id FROM [link:1] WHERE (SELECT id FROM box WHERE x = $parent.x);
