/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["compute-only"]

[test]
reason = """
	A divergence pinned in a pair rather than fixed. `ONLY` over an array
	source written without a limit of one is refused up front by the per-row
	engine, whatever the array turns out to hold, and answered by the
	streaming engine from the rows there actually were.

	The two rules agree everywhere the count settles it — a second row is an
	error on both, and one row or none is fine on both once a `LIMIT 1` is
	written. They part only where the source is an array and no limit bounds
	it: `ONLY [1]` plainly has one row, and `ONLY []` none, yet the per-row
	engine refuses both because an array *could* hold several.

	The streaming engine refuses by count instead, so it errors exactly when
	the statement really did answer with more than one row. Matching the other
	rule would mean refusing a statement whose answer is a single row, and a
	parameter makes the distinction unreachable anyway: `$empty` holding
	`[]` is substituted into the plan as the literal `[]` and cannot be told
	apart from one written there.
"""

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"
*/

-- An array of one row, and of none, with no limit written.
SELECT * FROM ONLY [1];
SELECT * FROM ONLY [];

-- A limit of one settles it on either engine.
SELECT * FROM ONLY [1] LIMIT 1;
SELECT * FROM ONLY [] LIMIT 1;

-- So does a source that is not an array.
SELECT * FROM ONLY 1;
SELECT * FROM ONLY NONE;

-- And a second row is an error either way.
SELECT * FROM ONLY [1, 2];
