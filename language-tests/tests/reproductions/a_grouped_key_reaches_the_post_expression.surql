/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A projection that combines an aggregate with a grouping key is answered
	with the group's own key value. The aggregate is folded per row and the
	rest of the expression is evaluated once for the group, against the
	group's answers rather than against a row — so a key named there is read
	from the group, not from a row that is gone by then.

	Which reads name a key is the rule that decides whether a whole projection
	is one: the key as written, or the alias a projected field gave it. A key
	is equally readable from inside an aggregate's argument, where it is
	evaluated per row like any other field, and the last two queries are the
	control for that and for an aggregate standing alone.

	The totals are not checked for numeric type, which
	`group_aggregate_numeric_type.surql` pins.
"""

[[test.results]]
value = "[{ g: 1, id: t:1, o: { n: 100 }, v: 10 }]"

[[test.results]]
value = "[{ g: 1, id: t:2, o: { n: 100 }, v: 20 }]"

[[test.results]]
value = "[{ g: 2, id: t:3, o: { n: 200 }, v: 5 }]"

[[test.results]]
value = "[{ g: 1, x: 31f }, { g: 2, x: 7f }]"

[[test.results]]
value = "[{ k: 1, x: 31f }, { k: 2, x: 7f }]"

[[test.results]]
value = "[{ o: { n: 100 }, x: 130f }, { o: { n: 200 }, x: 205f }]"

[[test.results]]
value = "[{ c: 2, g: 1, x: 61f }, { c: 1, g: 2, x: 12f }]"

[[test.results]]
value = "[{ g: 1, x: 32f }, { g: 2, x: 7f }]"

[[test.results]]
value = "[{ g: 1, s: 30f }, { g: 2, s: 5f }]"
*/

CREATE t:1 SET g = 1, v = 10, o = { n: 100 };
CREATE t:2 SET g = 1, v = 20, o = { n: 100 };
CREATE t:3 SET g = 2, v = 5, o = { n: 200 };

-- The key as written, beside the aggregate that reduces the group.
SELECT math::sum(v) + g AS x, g FROM t GROUP BY g;

-- The key named by the alias a projected field gave it.
SELECT g AS k, math::sum(v) + k AS x FROM t GROUP BY k;

-- A key that is a path into the row is read the same way.
SELECT o.n, math::sum(v) + o.n AS x FROM t GROUP BY o.n;

-- The aggregate need not be the whole of what the key is combined with.
SELECT (math::sum(v) * 2) + g AS x, count() AS c, g FROM t GROUP BY g;

-- Control: a key inside the aggregate's own argument is read per row.
SELECT g, math::sum(v + g) AS x FROM t GROUP BY g;

-- Control: an aggregate with nothing beside it is unchanged.
SELECT g, math::sum(v) AS s FROM t GROUP BY g;
