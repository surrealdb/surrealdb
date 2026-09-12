/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A count scan stands in for Source→Aggregate→Project, so it may only be
	chosen where the source it replaces yields the rows the scan counts. Two
	sources spell a range and are not the same source.

	`t:1..` is a record-id range: the source names a contiguous key range, and
	counting its keys is counting its rows.

	`(t:1)..` is an expression the plan evaluates at run time. Its value is a
	range, and a value source yields that value as *one row* — which is what
	`SELECT * FROM (t:1)..` shows, and what the full pipeline counts. Reading
	it as a key range asks `CountScan` for a source it cannot take, turning a
	valid count into `CountScan received a non-table source`.

	The shape is the same for `>..`, which skips the start, and the `VALUE`
	projection is included because the shortcut decides the answer's shape as
	well as its number and both are reached through the same match.

	The last statement is the source on its own, which is what makes the
	expected count of 1 legible rather than arbitrary.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

# A range *value* is one source row, so the count is 1 — not the 3 records the
# range would name if it were a key range.
[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[{ count: 1 }]"

# The record-id range source, which genuinely does name a key range.
[[test.results]]
value = "[{ count: 3 }]"

# The source alone: one row, holding the range.
[[test.results]]
value = "[t:1..]"
*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1, t:2, t:3 RETURN NONE;

SELECT count() FROM ((t:1)..) GROUP ALL;
SELECT VALUE count() FROM ((t:1)..) GROUP ALL;
SELECT count() FROM ((t:1)>..) GROUP ALL;
SELECT count() FROM t:1.. GROUP ALL;
SELECT * FROM (t:1)..;
