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
A LIMIT or START counts the rows the WHERE admits whatever shape the subquery
in the WHERE takes, read as a record user whose table permission reads `$auth`
(it sees the even n other than 4 of `ku`, whose rows scan in n order). The
subquery reads the same table or another, orders and limits what it reads,
skips rows of its own, sits among other conjuncts, under OR, behind IN and NOT
IN, reads the row it is evaluated for, or comes twice; the outer statement
pages with and without START and orders its own rows.
"""

# The subquery reads the table it is judged against.
[[test.results]]
value = "[8, 18, 28]"

[[test.results]]
value = "[18, 28]"

# The subquery reads a table whose permission reads nothing of the caller.
[[test.results]]
value = "[14, 24]"

[[test.results]]
value = "[24]"

[[test.results]]
value = "[0, 10, 20]"

# The subquery skips a row of its own.
[[test.results]]
value = "[2, 12, 22]"

[[test.results]]
value = "[12, 22]"

# Beside other conjuncts, in either order, and under OR.
[[test.results]]
value = "[10, 20]"

[[test.results]]
value = "[10, 20]"

[[test.results]]
value = "[2, 8, 18]"

# Behind IN and NOT IN.
[[test.results]]
value = "[0, 2, 10, 12, 20]"

[[test.results]]
value = "[10, 12, 20]"

[[test.results]]
value = "[6, 8, 14]"

# The outer statement orders its rows.
[[test.results]]
value = "[30, 20, 10]"

[[test.results]]
value = "[20, 10, 0]"

[[test.results]]
value = "[0, 10]"

# Two subqueries, one over each table.
[[test.results]]
value = "[10, 20]"

# A subquery that reads the row it is evaluated for.
[[test.results]]
value = "[0, 10, 20]"

# A projection of more than the value.
[[test.results]]
value = "[{ k: 0, n: 0 }, { k: 0, n: 10 }]"
*/

SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n DESC LIMIT 1)[0] LIMIT 3;
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n DESC LIMIT 1)[0] START 1 LIMIT 2;

SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM other ORDER BY n DESC LIMIT 1)[0] LIMIT 2;
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM other ORDER BY n DESC LIMIT 1)[0] START 1 LIMIT 1;
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM other ORDER BY n LIMIT 1)[0] LIMIT 3;

SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1 START 1)[0] LIMIT 3;
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1 START 1)[0] START 1 LIMIT 2;

SELECT VALUE n FROM ku WHERE n > 5 AND k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0] LIMIT 2;
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0] AND n > 5 LIMIT 2;
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n DESC LIMIT 1)[0] OR n = 2 LIMIT 3;

SELECT VALUE n FROM ku WHERE k IN (SELECT VALUE k FROM ku ORDER BY n LIMIT 2) LIMIT 5;
SELECT VALUE n FROM ku WHERE k IN (SELECT VALUE k FROM ku ORDER BY n LIMIT 2) START 2 LIMIT 3;
SELECT VALUE n FROM ku WHERE k NOT IN (SELECT VALUE k FROM ku ORDER BY n LIMIT 2) LIMIT 3;

SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0] ORDER BY n DESC LIMIT 3;
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0] ORDER BY n DESC START 1 LIMIT 3;
SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0] ORDER BY n LIMIT 2;

SELECT VALUE n FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0]
	AND n >= (SELECT VALUE n FROM other ORDER BY n DESC LIMIT 1)[0] - 29 LIMIT 2;

SELECT VALUE n FROM ku WHERE (SELECT VALUE n FROM ku WHERE n = $parent.n ORDER BY n LIMIT 1)[0] % 10 = 0 LIMIT 3;

SELECT n, k FROM ku WHERE k = (SELECT VALUE k FROM ku ORDER BY n LIMIT 1)[0] LIMIT 2;
