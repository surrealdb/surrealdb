/**
[env]
namespace = true
database = true

[test]
reason = """
	A write whose WHERE reads a subquery only for existence selects the rows the whole read selects,
	and the rows it writes or deletes are read back the same.
"""

[[test.results]]
value = "[{ hit: false, id: outer:1, x: 1 }]"

[[test.results]]
value = "[{ hit: false, id: outer:2, x: 2 }]"

[[test.results]]
value = "[{ hit: false, id: outer:3, x: 3 }]"

[[test.results]]
value = "[{ id: inner:1, x: 100 }]"

[[test.results]]
value = "[{ id: inner:2, x: 3 }]"

[[test.results]]
value = "[{ id: inner:3, x: 2 }]"

[[test.results]]
value = "[outer:2, outer:3]"

[[test.results]]
value = "[[outer:1, false], [outer:2, true], [outer:3, true]]"

[[test.results]]
value = "[{ hit: false, id: outer:1, x: 1 }, { hit: false, id: outer:2, x: 2 }, { hit: false, id: outer:3, x: 3 }]"

[[test.results]]
value = "[outer:1]"

[[test.results]]
value = "[[outer:1, true], [outer:2, false], [outer:3, false]]"

[[test.results]]
value = "[{ hit: false, id: outer:3, x: 3 }]"

[[test.results]]
value = "[outer:1, outer:2]"

[[test.results]]
value = "[NONE, NONE]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ found: [{ id: inner:2 }], id: outer:9, x: 3 }]"

[[test.results]]
value = "[{ found: false, id: outer:10, shown: 'yes', x: 2 }]"
*/

CREATE outer:1 SET x = 1, hit = false;
CREATE outer:2 SET x = 2, hit = false;
CREATE outer:3 SET x = 3, hit = false;
CREATE inner:1 SET x = 100;
CREATE inner:2 SET x = 3;
CREATE inner:3 SET x = 2;
UPDATE outer SET hit = true WHERE (SELECT id FROM inner WHERE x = $parent.x) RETURN VALUE id;
SELECT VALUE [id, hit] FROM outer;
UPDATE outer SET hit = false;
UPDATE outer SET hit = true WHERE !(SELECT id FROM inner WHERE x = $parent.x) RETURN VALUE id;
SELECT VALUE [id, hit] FROM outer;
DELETE outer WHERE (SELECT id FROM inner WHERE x = $parent.x AND x > 2) RETURN BEFORE;
SELECT VALUE id FROM outer;
DELETE outer WHERE !(SELECT id FROM inner WHERE x = 99) RETURN VALUE id;
SELECT VALUE id FROM outer;
-- Written by a statement that reads in its own value.
CREATE outer:9 SET x = 3, found = (SELECT id FROM inner WHERE x = 3);
CREATE outer:10 SET x = 2, found = !(SELECT id FROM inner WHERE x = 2), shown = IF (SELECT id FROM inner WHERE x = 2) { 'yes' } ELSE { 'no' };
