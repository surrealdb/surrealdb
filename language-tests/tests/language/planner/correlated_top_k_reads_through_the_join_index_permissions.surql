/**
[env]
imports = ["language/planner/correlated_top_k_reads_through_the_join_index_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	Record-user companion of correlated_top_k_reads_through_the_join_index:
	the reader's select permission on `child` hides the rows with `day = 1`.
	A permission stands between the scan and the join predicate, so the
	statement reads through the join index as it does for any permissioned
	table; each statement answers what the same statement read through its
	table (`WITH NOINDEX`) answers, the hidden rows absent from both. Each
	statement answers true.
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
