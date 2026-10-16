/**
[env]
namespace = true
database = true

[test]
reason = """
	AND and OR answer one of their operands, not a boolean, so a subquery on either side of one is
	cut to a row only where the operator's own result is read for truthiness. Where the operator's
	value, a count, a length, a membership test or a comparison reads the subquery, the subquery
	still holds every row.
"""

[[test.results]]
value = "[{ id: outer:1, n: 1, x: 1 }]"

[[test.results]]
value = "[{ id: outer:2, n: 2, x: 2 }]"

[[test.results]]
value = "[{ id: outer:3, n: 3, x: 3 }]"

[[test.results]]
value = "[{ id: inner:1, x: 3, y: 1 }]"

[[test.results]]
value = "[{ id: inner:2, x: 3, y: 2 }]"

[[test.results]]
value = "[{ id: inner:3, x: 2, y: 3 }]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[{ id: inner:1 }, { id: inner:2 }, { id: inner:3 }]"

[[test.results]]
value = "[{ id: inner:2 }, { id: inner:3 }]"

[[test.results]]
value = "[5]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[0, [{ id: inner:3 }], [{ id: inner:1 }, { id: inner:2 }]]"

[[test.results]]
value = "[[], 1, 1]"

[[test.results]]
value = "[outer:1]"

[[test.results]]
value = "[outer:3]"

[[test.results]]
value = "[outer:3]"

[[test.results]]
value = "[outer:2, outer:3]"

[[test.results]]
value = "[outer:1]"

[[test.results]]
value = "[outer:3]"

[[test.results]]
value = "[outer:3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[[[]], [[{ id: inner:3 }]], [[{ id: inner:1 }, { id: inner:2 }]]]"

[[test.results]]
value = "3"

[[test.results]]
value = "3"

[[test.results]]
value = "false"
*/

CREATE outer:1 SET x = 1, n = 1;
CREATE outer:2 SET x = 2, n = 2;
CREATE outer:3 SET x = 3, n = 3;
CREATE inner:1 SET x = 3, y = 1;
CREATE inner:2 SET x = 3, y = 2;
CREATE inner:3 SET x = 2, y = 3;
RETURN (SELECT id FROM inner) AND [1];
RETURN [] OR (SELECT id FROM inner);
RETURN [] OR (SELECT id FROM inner WHERE y > 1);
RETURN (SELECT id FROM inner WHERE y > 99) OR [5];
RETURN (SELECT id FROM inner WHERE y > 99) AND [5];
SELECT VALUE ((SELECT id FROM inner WHERE x = $parent.x) OR 0) FROM outer;
SELECT VALUE ((SELECT id FROM inner WHERE x = $parent.x) AND 1) FROM outer;
SELECT VALUE id FROM outer WHERE ((SELECT id FROM inner WHERE x = $parent.x) OR 0) = 0;
SELECT VALUE id FROM outer WHERE count((SELECT id FROM inner WHERE x = $parent.x)) > 1;
SELECT VALUE id FROM outer WHERE array::len((SELECT id FROM inner WHERE x = $parent.x)) = 2;
SELECT VALUE id FROM outer WHERE x IN (SELECT VALUE x FROM inner);
SELECT VALUE id FROM outer WHERE x NOT IN (SELECT VALUE x FROM inner);
SELECT VALUE id FROM outer WHERE (SELECT VALUE y FROM inner WHERE x = $parent.x) = [1, 2];
SELECT VALUE id FROM outer WHERE inner:1 IN (SELECT VALUE id FROM inner WHERE x = $parent.x);
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WHERE x = $parent.x) CONTAINS inner:2;
SELECT VALUE (SELECT id FROM inner WHERE x = $parent.x) ?? 'none' FROM outer:1;
SELECT VALUE [(SELECT id FROM inner WHERE x = $parent.x)] FROM outer;
RETURN array::len((SELECT id FROM inner));
RETURN count((SELECT id FROM inner));
RETURN (SELECT id FROM inner) = [];
