/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	An `ORDER BY` key that names a projection output sorts by what that output
	holds, so `b AS a … ORDER BY a` orders by `b`. An index over the record's
	own `a` therefore does not supply the ordering, and the sort cannot be
	eliminated against it.

	The rows below are the same before and after `DEFINE INDEX`: defining an
	index over `a` does not change the ordering.
"""

[[test.results]]
value = "[{ a: 3, b: 1, id: r:1 }]"

[[test.results]]
value = "[{ a: 2, b: 2, id: r:2 }]"

[[test.results]]
value = "[{ a: 1, b: 3, id: r:3 }]"

[[test.results]]
value = "[{ a: 1 }, { a: 2 }, { a: 3 }]"

[[test.results]]
value = "[{ a: 1, z: 3 }, { a: 2, z: 2 }, { a: 3, z: 1 }]"

[[test.results]]
value = "[{ a: 1, b: 3 }, { a: 2, b: 2 }, { a: 3, b: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1 }, { a: 2 }, { a: 3 }]"

[[test.results]]
value = "[{ a: 1, z: 3 }, { a: 2, z: 2 }, { a: 3, z: 1 }]"

[[test.results]]
value = "[{ a: 1, b: 3 }, { a: 2, b: 2 }, { a: 3, b: 1 }]"

[[test.results]]
value = "[{ a: 1 }, { a: 2 }, { a: 3 }]"

[[test.results]]
value = "[{ a: 3 }, { a: 2 }, { a: 1 }]"

[[test.results]]
value = "[{ a: 3, b: 1 }, { a: 2, b: 2 }, { a: 1, b: 3 }]"
*/

-- `a` descends as `b` ascends, so an ordering taken from the wrong one of
-- them is visible in the rows.
CREATE r:1 SET a = 3, b = 1;
CREATE r:2 SET a = 2, b = 2;
CREATE r:3 SET a = 1, b = 3;

-- A rename putting `b` under the name `a`: the sort follows the projected
-- value, not the record's own field of that name.
SELECT b AS a FROM r ORDER BY a;
SELECT b AS a, a AS z FROM r ORDER BY a;

-- The key is selected under its own name, so it orders by the record's `a`.
SELECT a, b FROM r ORDER BY a;

-- An index over `a` must not change any of the three.
DEFINE INDEX r_a ON r FIELDS a;
SELECT b AS a FROM r ORDER BY a;
SELECT b AS a, a AS z FROM r ORDER BY a;
SELECT a, b FROM r ORDER BY a;

-- The renamed key read again through an outer statement.
SELECT * FROM (SELECT b AS a FROM r ORDER BY a) ORDER BY a;

-- A self-alias and an unprojected key still take the record's `a`.
SELECT a AS a FROM r ORDER BY a DESC;
SELECT a, b FROM r ORDER BY a DESC;
