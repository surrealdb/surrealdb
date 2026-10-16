/**
[env]
imports = ["./permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
	The extreme of an indexed `int` field read as a record user: a table permission hides the row
	that holds the extreme, so the extreme of the rows the user may read is answered; a table that
	denies every read answers as an empty one; a field permission that restricts the field, and
	one that restricts another, answer as the fold over the visible rows does.
"""

[[test.results]]
value = "[{ m: 50 }]"

[[test.results]]
value = "[{ m: -10 }]"

[[test.results]]
value = "[50]"

[[test.results]]
value = "[-10]"

[[test.results]]
value = "50"

[[test.results]]
value = "[{ id: open:1, m: -10 }, { id: open:2, m: -10 }]"

[[test.results]]
value = "[{ m: 50 }]"

[[test.results]]
value = "[{ m: -Infinity }]"

[[test.results]]
value = "[{ m: Infinity }]"

[[test.results]]
value = "[-Infinity]"

[[test.results]]
value = "[{ m: 6 }]"

[[test.results]]
value = "[{ m: -2 }]"

[[test.results]]
value = "[{ m: 20 }]"

[[test.results]]
value = "[{ m: -5 }]"

[[test.results]]
value = "[{ m: 30 }]"

[[test.results]]
value = "[{ m: -30 }]"

[[test.results]]
value = "[{ m: 4 }]"

[[test.results]]
value = "[{ m: 1 }]"

[[test.results]]
value = "[{ m: 12 }]"

[[test.results]]
value = "[{ m: 5 }]"
*/

SELECT math::max(n) AS m FROM pub GROUP ALL;
SELECT math::min(n) AS m FROM pub GROUP ALL;
SELECT VALUE math::max(n) FROM pub GROUP ALL;
SELECT VALUE math::min(n) FROM pub GROUP ALL;
RETURN (SELECT VALUE math::max(n) FROM pub GROUP ALL)[0];
SELECT id, (SELECT VALUE math::min(n) FROM pub GROUP ALL)[0] AS m FROM open ORDER BY id LIMIT 2;
SELECT math::max(n) AS m FROM pub GROUP ALL LIMIT 1;
SELECT math::max(n) AS m FROM denied GROUP ALL;
SELECT math::min(n) AS m FROM denied GROUP ALL;
SELECT VALUE math::max(n) FROM denied GROUP ALL;
SELECT math::max(n) AS m FROM open GROUP ALL;
SELECT math::min(n) AS m FROM open GROUP ALL;
SELECT math::max(n) AS m FROM owned GROUP ALL;
SELECT math::min(n) AS m FROM owned GROUP ALL;
SELECT math::max(n) AS m FROM guarded GROUP ALL;
SELECT math::min(n) AS m FROM guarded GROUP ALL;
SELECT math::max(m) AS m FROM guarded GROUP ALL;
SELECT math::min(m) AS m FROM guarded GROUP ALL;
SELECT math::max(n) AS m FROM mixed GROUP ALL;
SELECT math::min(n) AS m FROM mixed GROUP ALL;
