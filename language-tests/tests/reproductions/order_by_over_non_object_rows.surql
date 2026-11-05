/**
[env]
namespace = true
database = true

[test]
reason = """
An ORDER BY over rows that are not objects. A sort key that reads a field of a
row that is a scalar reads NONE, and the rows keep their order among equal keys.
A sort over a source of records reads the fields of each record as before. The
projection of the sorted rows follows the order the sort gives.
"""

[[test.results]]
value = "[5]"

[[test.results]]
value = "[5, { y: 1 }]"

[[test.results]]
value = "[5, 3, 1]"

[[test.results]]
value = "[NONE, 1]"

[[test.results]]
value = "[5, { x: 1 }]"

[[test.results]]
value = "[3, 1]"

[[test.results]]
value = "[{ y: 1 }, 5]"

[[test.results]]
value = "[5, { x: 1 }]"

[[test.results]]
value = "[{ x: 1 }, { x: 2 }]"

*/
SELECT x FROM [5] ORDER BY x;
SELECT x AS y FROM [5, { x: 1 }] ORDER BY y;
SELECT x FROM [5, 3, 1] ORDER BY x;
SELECT VALUE x FROM [5, { x: 1 }] ORDER BY x;
SELECT x FROM [5, { x: 1 }] ORDER BY x;
SELECT x FROM [5, 3, 1] ORDER BY x LIMIT 2 START 1;
SELECT x AS y FROM [5, { x: 1 }] ORDER BY x DESC;
SELECT * FROM [5, { x: 1 }] ORDER BY x;
SELECT x FROM [{ x: 2 }, { x: 1 }] ORDER BY x;
