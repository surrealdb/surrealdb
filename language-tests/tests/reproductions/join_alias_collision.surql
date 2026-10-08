/**
[env]
namespace = true
database = true

[test]
reason = """
	A join minted for a decorrelated subquery names the matched right row on
	the joined row, and the join's residual is judged over that row, so the
	name must not be one the statement reads: a field named like a minted
	alias, reached through `$parent`, would be read as the right row instead
	of as the field. The alias is minted above every such name, so the
	comparison reads the left row's value.
"""

[[test.results]]
value = "[{ _join0: 5, id: t:1, team: 'a' }]"

[[test.results]]
value = "[{ _join0: 50, id: t:2, team: 'b' }]"

[[test.results]]
value = "[{ id: member:1, n: 10, team: 'a' }]"

[[test.results]]
value = "[{ id: member:2, n: 10, team: 'b' }]"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "[{ id: t:2 }]"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"
*/

-- `_join0` is the name the first join minted in a plan carries.
CREATE t:1 SET team = 'a', _join0 = 5;
CREATE t:2 SET team = 'b', _join0 = 50;
CREATE member:1 SET team = 'a', n = 10;
CREATE member:2 SET team = 'b', n = 10;

-- The residual reads `_join0` off the left row: t:1 passes (10 > 5), t:2
-- does not (10 > 50 is false).
SELECT id FROM t WHERE team IN (SELECT VALUE team FROM member WHERE n > $parent._join0);

-- The reverse comparison keeps the other row.
SELECT id FROM t WHERE team IN (SELECT VALUE team FROM member WHERE n < $parent._join0);

-- The field is the stored value where no join stands.
SELECT id FROM t WHERE _join0 > 1;
