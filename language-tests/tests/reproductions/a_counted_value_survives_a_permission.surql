/**
[env]
imports = ["reproductions/a_counted_value_survives_a_permission_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = """
	The permission arms of `a_counted_value_is_the_count_itself`, which
	need an identity the table's `PERMISSIONS` can refuse.

	A count scan resolves the table's `SELECT` permission itself, and answers
	two of the three outcomes without ever reaching the key range: a table the
	caller may not see counts nothing, and a per-row permission sends it to a
	scan that counts what survives. Both emit a count the same way the fast
	path does, so both have to answer in the shape the statement asked for —
	`VALUE` included.
"""

[[test.results]]
value = "[0]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[{ count: 2 }]"

*/

-- Denied outright: the table is invisible, and a count of nothing is still
-- the count the statement asked for the shape of.
SELECT VALUE count() FROM hidden GROUP ALL;
SELECT count() FROM hidden GROUP ALL;

-- Judged per row: the scan the count falls back to counts what survives.
SELECT VALUE count() FROM part GROUP ALL;
SELECT count() FROM part GROUP ALL;
