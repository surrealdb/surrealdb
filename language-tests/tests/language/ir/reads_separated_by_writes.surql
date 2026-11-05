/**
[env]
namespace = true
database = true

[test]
reason = """
A read of a record sees the writes made to it before the read and not those made after:
two syntactically equal reads of one record around a write, whether a nested UPDATE or a write in a
loop body, answer different values. The rows of the loop's CREATE are sorted because their order is
the order of random record ids.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1, [1, 2]]"

[[test.results]]
value = "[3, NONE, [3, 4]]"
*/

CREATE t:1 SET n = 1 RETURN NONE;
CREATE person:1, person:2 RETURN NONE;
RETURN [t:1.n, (UPDATE person SET a = t:1.n, b = (UPDATE t:1 SET n += 1 RETURN NONE) RETURN VALUE a)];
RETURN [t:1.n, (FOR $i IN [1, 2] { CREATE log SET v = t:1.n; UPDATE t:1 SET n += 1; }), array::sort((SELECT VALUE v FROM log))];
