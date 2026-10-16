/**
[env]
namespace = true
database = true

[test]
reason = """
	Only a plain select is cut to its first row: an ONLY, a GROUP, an ORDER BY, a LIMIT, a START, a
	SPLIT, a FETCH, a TIMEOUT or a WITH hint changes the rows the subquery yields or what reading
	them costs, so each keeps its read whole in a truthiness position and answers as it does.
"""

[[test.results]]
value = "[{ id: outer:1, x: 1 }]"

[[test.results]]
value = "[{ id: outer:2, x: 2 }]"

[[test.results]]
value = "[{ id: outer:3, x: 3 }]"

[[test.results]]
value = "[{ id: inner:1, link: outer:1, tags: ['a', 'b'], x: 1 }]"

[[test.results]]
value = "[{ id: inner:2, link: outer:2, tags: [], x: 2 }]"

[[test.results]]
value = "[{ id: inner:3, link: outer:3, tags: ['c'], x: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[outer:1, outer:2, outer:3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[outer:1, outer:2]"

[[test.results]]
value = "[outer:2]"

[[test.results]]
value = "[outer:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[outer:1, outer:2]"

[[test.results]]
value = "[outer:1, outer:2, outer:3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[outer:1, outer:2]"

[[test.results]]
value = "[outer:1, outer:2, outer:3]"

[[test.results]]
value = "[outer:1, outer:2]"

[[test.results]]
value = "[outer:1, outer:2]"

[[test.results]]
value = "[outer:1, outer:2]"

[[test.results]]
value = "[outer:1, outer:2]"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"
*/

CREATE outer:1 SET x = 1;
CREATE outer:2 SET x = 2;
CREATE outer:3 SET x = 3;
CREATE inner:1 SET x = 1, tags = ['a', 'b'], link = outer:1;
CREATE inner:2 SET x = 2, tags = [], link = outer:2;
CREATE inner:3 SET x = 2, tags = ['c'], link = outer:3;
DEFINE INDEX inner_x ON inner FIELDS x;
SELECT VALUE id FROM outer WHERE (SELECT id FROM ONLY inner:1);
SELECT VALUE id FROM outer WHERE (SELECT id FROM ONLY inner:99);
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WHERE x = $parent.x LIMIT 1);
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WHERE x = $parent.x START 1);
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WHERE x = $parent.x START 1 LIMIT 3);
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner LIMIT 0);
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WHERE x = $parent.x ORDER BY id DESC);
SELECT VALUE id FROM outer WHERE (SELECT count() FROM inner WHERE x = 99 GROUP ALL);
SELECT VALUE id FROM outer WHERE (SELECT x FROM inner WHERE x = 99 GROUP BY x);
SELECT VALUE id FROM outer WHERE (SELECT tags FROM inner WHERE x = $parent.x SPLIT tags);
SELECT VALUE id FROM outer WHERE (SELECT tags FROM inner SPLIT tags);
SELECT VALUE id FROM outer WHERE (SELECT link FROM inner WHERE x = $parent.x FETCH link);
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WHERE x = $parent.x TIMEOUT 5s);
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WITH NOINDEX WHERE x = $parent.x);
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WITH INDEX inner_x WHERE x = $parent.x);
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WHERE x = $parent.x FOR UPDATE);
