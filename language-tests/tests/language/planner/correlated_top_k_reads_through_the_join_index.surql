/**
[env]
imports = ["language/planner/correlated_top_k_reads_through_the_join_index_import.surql"]
auth = { level = "owner" }

[test]
reason = """
	A correlated statement in value position that reads the first rows of an
	order (`ORDER BY .. LIMIT`) through an equality on an indexed field of its
	row and `$parent` answers what the same statement read through its table
	(`WITH NOINDEX`) answers: rows tied on the sort key, a descending and a
	two-key order, more parents than a lookup serves before its hashed plan
	takes over, the statement in a projection, in a `WHERE` and in an
	`UPDATE .. SET`. Each of those answers true; the last two statements pin
	the rows themselves for the tied and the `START` cases.
"""

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ n: 1, top: [1, 3] }, { n: 2, top: [3, 0] }, { n: 3, top: [2, 1] }]"

[[test.results]]
value = "[{ n: 1, top: [{ at: 3, day: 1 }, { at: 0, day: 1 }, { at: 1, day: 2 }] }, { n: 2, top: [{ at: 2, day: 1 }, { at: 3, day: 2 }, { at: 0, day: 2 }] }]"

*/

(SELECT n, (SELECT VALUE at FROM child WHERE owner = $parent.id ORDER BY day DESC LIMIT 2) AS top FROM parent ORDER BY n)
	== (SELECT n, (SELECT VALUE at FROM child WITH NOINDEX WHERE owner = $parent.id ORDER BY day DESC LIMIT 2) AS top FROM parent ORDER BY n);

(SELECT n, (SELECT VALUE at FROM child WHERE owner = $parent.id ORDER BY day, at DESC LIMIT 1)[0] AS top FROM parent ORDER BY n)
	== (SELECT n, (SELECT VALUE at FROM child WITH NOINDEX WHERE owner = $parent.id ORDER BY day, at DESC LIMIT 1)[0] AS top FROM parent ORDER BY n);

(SELECT VALUE n FROM parent WHERE (SELECT VALUE day FROM child WHERE owner = $parent.id ORDER BY day DESC LIMIT 1)[0] = 2 ORDER BY n)
	== (SELECT VALUE n FROM parent WHERE (SELECT VALUE day FROM child WITH NOINDEX WHERE owner = $parent.id ORDER BY day DESC LIMIT 1)[0] = 2 ORDER BY n);

(SELECT n, (SELECT at, day FROM child WHERE owner = $parent.id ORDER BY day LIMIT 3 START 1) AS top FROM parent ORDER BY n)
	== (SELECT n, (SELECT at, day FROM child WITH NOINDEX WHERE owner = $parent.id ORDER BY day LIMIT 3 START 1) AS top FROM parent ORDER BY n);

(UPDATE parent SET a = (SELECT VALUE at FROM child WHERE owner = $parent.id ORDER BY day DESC LIMIT 1)[0] RETURN VALUE a)
	== (UPDATE parent SET b = (SELECT VALUE at FROM child WITH NOINDEX WHERE owner = $parent.id ORDER BY day DESC LIMIT 1)[0] RETURN VALUE b);

SELECT n, (SELECT VALUE at FROM child WHERE owner = $parent.id ORDER BY day DESC LIMIT 2) AS top FROM parent WHERE n <= 3 ORDER BY n;

SELECT n, (SELECT at, day FROM child WHERE owner = $parent.id ORDER BY day LIMIT 3 START 1) AS top FROM parent WHERE n <= 2 ORDER BY n;
