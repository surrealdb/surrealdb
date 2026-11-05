/**
[env]
namespace = true
database = true

[test]
reason = """
	A subquery that only a truthiness test reads (a WHERE, a NOT, an IF condition, either side of
	an AND or an OR that is itself so read) is answered by its first row, and it is answered the
	same as when every row is read. Each form has a twin that projects the subquery's value, which
	still holds every row. The first rows of `inner` match no row of `outer`, and the rows that
	match lie late in scan order, so a probe that cut the whole inner read instead of each
	probe would lose them.
"""

[[test.results]]
value = "[{ id: outer:1, n: 1, x: 1 }]"

[[test.results]]
value = "[{ id: outer:2, n: 2, x: 2 }]"

[[test.results]]
value = "[{ id: outer:3, n: 3, x: 3 }]"

[[test.results]]
value = "[{ id: outer:4, n: 4, x: 4 }]"

[[test.results]]
value = "[{ id: outer:5, n: 5, x: 5 }]"

[[test.results]]
value = "[{ id: inner:1, x: 100, y: 0 }]"

[[test.results]]
value = "[{ id: inner:2, x: 101, y: 0 }]"

[[test.results]]
value = "[{ id: inner:3, x: 3, y: 1 }]"

[[test.results]]
value = "[{ id: inner:4, x: 5, y: 1 }]"

[[test.results]]
value = "[{ id: inner:5, x: 2, y: 2 }]"

[[test.results]]
value = "[outer:1, outer:2, outer:3, outer:4, outer:5]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[{ id: inner:3 }, { id: inner:4 }]]"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[outer:2, outer:3, outer:5]"

[[test.results]]
value = "[[], [{ id: inner:5 }], [{ id: inner:3 }], [], [{ id: inner:4 }]]"

[[test.results]]
value = "[outer:1, outer:2, outer:3, outer:4, outer:5]"

[[test.results]]
value = "[outer:4, outer:5]"

[[test.results]]
value = "[[{ id: inner:3 }, { id: inner:4 }, { id: inner:5 }], [{ id: inner:3 }, { id: inner:4 }], [{ id: inner:4 }], [{ id: inner:4 }], []]"

[[test.results]]
value = "[outer:1, outer:4]"

[[test.results]]
value = "[outer:1, outer:2, outer:3, outer:4, outer:5]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[outer:1, outer:4]"

[[test.results]]
value = "[true, false, false, true, false]"

[[test.results]]
value = "['miss', 'hit', 'hit', 'miss', 'hit']"

[[test.results]]
value = "['second']"

[[test.results]]
value = "'hit'"

[[test.results]]
value = "[outer:3, outer:5]"

[[test.results]]
value = "[outer:3, outer:5]"

[[test.results]]
value = "[outer:2, outer:3, outer:4, outer:5]"

[[test.results]]
value = "[outer:2, outer:3, outer:4, outer:5]"

[[test.results]]
value = "[outer:2, outer:3, outer:5]"

[[test.results]]
value = "[outer:4]"

[[test.results]]
value = "[outer:2, outer:3, outer:5]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[{ id: inner:3 }, { id: inner:4 }], false]"

[[test.results]]
value = "[[[], true], [[{ id: inner:5 }], false], [[{ id: inner:3 }], false], [[], true], [[{ id: inner:4 }], false]]"
*/

CREATE outer:1 SET x = 1, n = 1;
CREATE outer:2 SET x = 2, n = 2;
CREATE outer:3 SET x = 3, n = 3;
CREATE outer:4 SET x = 4, n = 4;
CREATE outer:5 SET x = 5, n = 5;
CREATE inner:1 SET x = 100, y = 0;
CREATE inner:2 SET x = 101, y = 0;
CREATE inner:3 SET x = 3, y = 1;
CREATE inner:4 SET x = 5, y = 1;
CREATE inner:5 SET x = 2, y = 2;
-- Uncorrelated.
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WHERE y = 1);
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WHERE y = 99);
SELECT VALUE (SELECT id FROM inner WHERE y = 1) FROM outer:1;
SELECT VALUE (SELECT id FROM inner WHERE y = 99) FROM outer:1;
-- Correlated by equality.
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WHERE x = $parent.x);
SELECT VALUE (SELECT id FROM inner WHERE x = $parent.x) FROM outer;
-- Correlated, with no join key.
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WHERE x > $parent.x);
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WHERE x < $parent.x AND y = 1);
SELECT VALUE (SELECT id FROM inner WHERE x > $parent.x AND y > 0) FROM outer;
-- Negated.
SELECT VALUE id FROM outer WHERE !(SELECT id FROM inner WHERE x = $parent.x);
SELECT VALUE id FROM outer WHERE !(SELECT id FROM inner WHERE y = 99);
SELECT VALUE id FROM outer WHERE !(SELECT id FROM inner WHERE y = 1);
SELECT VALUE id FROM outer WHERE NOT (SELECT id FROM inner WHERE x = $parent.x);
SELECT VALUE !(SELECT id FROM inner WHERE x = $parent.x) FROM outer;
-- In an IF.
SELECT VALUE IF (SELECT id FROM inner WHERE x = $parent.x) { 'hit' } ELSE { 'miss' } FROM outer;
SELECT VALUE IF (SELECT id FROM inner WHERE y = 99) { 'hit' } ELSE IF (SELECT id FROM inner WHERE y = 1) { 'second' } ELSE { 'miss' } FROM outer:1;
RETURN IF (SELECT id FROM inner WHERE y = 1) { 'hit' } ELSE { 'miss' };
-- Beside other conjuncts and disjuncts.
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WHERE x = $parent.x) AND n > 2;
SELECT VALUE id FROM outer WHERE n > 2 AND (SELECT id FROM inner WHERE x = $parent.x);
SELECT VALUE id FROM outer WHERE n > 3 OR (SELECT id FROM inner WHERE x = $parent.x);
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WHERE x = $parent.x) OR n > 3;
SELECT VALUE id FROM outer WHERE n > 1 AND (n > 4 OR (SELECT id FROM inner WHERE x = $parent.x));
SELECT VALUE id FROM outer WHERE !(SELECT id FROM inner WHERE x = $parent.x) AND n > 1;
SELECT VALUE id FROM outer WHERE ((SELECT id FROM inner WHERE x = $parent.x) OR 0);
-- The subquery's value read by a twin that keeps it whole.
LET $v = (SELECT id FROM inner WHERE y = 1);
RETURN [$v, !$v];
SELECT VALUE [(SELECT id FROM inner WHERE x = $parent.x), !(SELECT id FROM inner WHERE x = $parent.x)] FROM outer;
