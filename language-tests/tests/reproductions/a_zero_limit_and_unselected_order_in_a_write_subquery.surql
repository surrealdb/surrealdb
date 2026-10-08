/**
[env]
namespace = true
database = true

[test]
reason = """
A `SELECT` written in a write's clause answers as the same `SELECT` does at the
top level:

- a zero `LIMIT` reads no record, so a `WHERE` that would raise is not
  evaluated;
- an `ORDER BY` on a field the projection does not select is accepted;
- a sum over no rows is an integer.

The top-level forms are main's answers. Main answers the subquery forms
differently: it raises the `WHERE` error, refuses the `ORDER BY` with a message
naming its planner strategy, and sums no rows to a float.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 'x', id: t:1 }]"

[[test.results]]
value = "[{ a: 2, b: 'y', id: t:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: u:1, x: [] }]"

[[test.results]]
value = "[{ id: u:2, x: [{ a: 1 }, { a: 2 }] }]"

[[test.results]]
value = "[{ id: u:3, x: [{ s: true }] }]"

[[test.results]]
value = "[{ a: 1 }, { a: 2 }]"
*/

DEFINE TABLE t;
CREATE t:1 SET a = 1, b = 'x';
CREATE t:2 SET a = 2, b = 'y';
SELECT * FROM t WHERE <int>'x' = 1 LIMIT 0;
SELECT * FROM t WHERE <int>'x' = 1 LIMIT 1;
SELECT * FROM t LIMIT 0;
CREATE u:1 SET x = (SELECT * FROM t WHERE <int>'x' = 1 LIMIT 0);
CREATE u:2 SET x = (SELECT a FROM t ORDER BY b);
CREATE u:3 SET x = (SELECT type::is_int(math::sum(a)) AS s FROM [] GROUP ALL);
SELECT a FROM t ORDER BY b;
