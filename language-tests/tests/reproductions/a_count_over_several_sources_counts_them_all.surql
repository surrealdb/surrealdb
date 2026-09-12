/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	`SELECT count() … GROUP ALL` over several sources counts the rows of all
	of them. The per-row engine answers such a statement from a count taken
	per source, one row standing for each; that count is added to the group's
	rather than replacing it, which is what makes the answer the total instead
	of whatever the last source happened to hold.
"""

[[test.results]]
value = "[{ id: e:1 }, { id: e:2 }, { id: e:3 }]"

[[test.results]]
value = "[{ id: f:1 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ count: 5 }]"
*/

CREATE e:1, e:2, e:3;
CREATE f:1;

SELECT count() FROM e GROUP ALL;
SELECT count() FROM f GROUP ALL;

-- The total, whichever order the sources are written in.
SELECT count() FROM e, f GROUP ALL;
SELECT count() FROM f, e GROUP ALL;

-- A source named twice is counted twice.
SELECT count() FROM e, f, f GROUP ALL;

-- Mixing a table with rows written down counts both.
SELECT count() FROM e, [{ k: 1 }, { k: 2 }] GROUP ALL;
