/**
[env]
namespace = true
database = true
imports = ["reproductions/limit_over_subquery_where_record_user_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
reason = """
A LIMIT or START counts the rows the WHERE admits, not the rows a subquery in
the WHERE was evaluated for. The table's select permission reads a field of
`$auth`, so the caller sees the even n other than its own `c` (4), and the
WHERE holds a subquery over the same table with a limit of its own: it answers
k = 0, the k of the first row the caller sees. Each of the 19 visible rows
evaluates that subquery, and the rows left are n = 0, 10, 20 and 30, however
the LIMIT is written.
"""

[[test.results]]
value = "4"

# `u` takes random record ids, so the rows come in any order.
[[test.results]]
match = "array::sort($result) = [0, 10, 20, 30]"

[[test.results]]
match = "array::sort($result) = [0, 10, 20, 30]"

[[test.results]]
value = "[0, 10, 20, 30]"

[[test.results]]
value = "[0, 10, 20, 30]"

[[test.results]]
value = "[0, 10, 20]"

[[test.results]]
value = "[0, 10]"

[[test.results]]
value = "[0]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[10, 20, 30]"

[[test.results]]
value = "[20, 30]"

[[test.results]]
value = "[20]"

[[test.results]]
value = "[30]"

[[test.results]]
value = "[]"
*/

RETURN $auth.c;

-- Random ids: whichever rows a LIMIT reached first, the answer is all four.
SELECT VALUE n FROM u WHERE k = (SELECT VALUE k FROM u ORDER BY n LIMIT 1)[0] LIMIT 10;
SELECT VALUE n FROM u WHERE k = (SELECT VALUE k FROM u ORDER BY n LIMIT 1)[0] LIMIT 5;

-- Scan order: the LIMIT stops once it has taken the rows the WHERE admits.
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0] LIMIT 10;
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0];
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0] LIMIT 3;
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0] LIMIT 2;
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0] LIMIT 1;
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0] LIMIT 0;

-- START skips rows the WHERE admits, not rows that evaluated the subquery.
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0] START 1 LIMIT 10;
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0] START 2 LIMIT 10;
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0] START 2 LIMIT 1;
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0] START 3;
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0] START 4;
