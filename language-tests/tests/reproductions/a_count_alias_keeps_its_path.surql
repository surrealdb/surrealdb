/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	An alias places a count where it says, and a shortcut that answers a count
	without materialising a row places it in the same place. `AS foo.bar` names
	a path and nests, `` AS `foo.bar` `` names one key and stays flat: which of
	the two was written is decided by the statement, never by the plan that
	answers it.

	The count shortcut builds its row as a flat map of name to value, so it
	declines a statement whose alias names a path and the ordinary aggregation
	answers that one instead — `a_count_alias_keeps_its_path_new_executor`
	is where the two plans are pinned. Both shapes are here with and without a
	`WHERE`, since a whole-range count and an indexed count are separate
	shortcuts.
"""

[[test.results]]
value = "[{ id: t:1, k: 1 }]"

[[test.results]]
value = "[{ id: t:2, k: 1 }]"

[[test.results]]
value = "[{ id: t:3, k: 2 }]"

[[test.results]]
value = "[{ foo: { bar: 3 } }]"

[[test.results]]
value = "[{ foo: { bar: 2 } }]"

[[test.results]]
value = '[{ "foo.bar": 3 }]'

[[test.results]]
value = '[{ "foo.bar": 2 }]'

[[test.results]]
value = "[{ n: 3 }]"

[[test.results]]
value = "[{ n: 2 }]"
*/

CREATE t:1 SET k = 1;
CREATE t:2 SET k = 1;
CREATE t:3 SET k = 2;

-- A path nests.
SELECT count() AS foo.bar FROM t GROUP ALL;
SELECT count() AS foo.bar FROM t WHERE k = 1 GROUP ALL;

-- A quoted key with a dot in it is one key.
SELECT count() AS `foo.bar` FROM t GROUP ALL;
SELECT count() AS `foo.bar` FROM t WHERE k = 1 GROUP ALL;

-- Control: a plain alias, which both the shortcut and the aggregation place
-- the same way.
SELECT count() AS n FROM t GROUP ALL;
SELECT count() AS n FROM t WHERE k = 1 GROUP ALL;
