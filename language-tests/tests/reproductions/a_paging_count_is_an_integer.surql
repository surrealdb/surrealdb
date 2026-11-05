/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	`LIMIT` and `START` each name a count of rows, and a count is written down
	as a non-negative integer that fits in a `u32`. Nothing else is read as
	one: a float or decimal is rejected rather than truncated to the integer
	below it, `NONE` and `NULL` are rejected rather than read as zero, and a
	number too large to fit is rejected rather than wrapped — so the rows a
	statement returns do not depend on the width of a pointer on the machine
	running it.

	The clause means the same thing wherever it is applied, so a scan that
	takes the paging for itself reads it exactly as the operator above one
	does, and an error raised while evaluating the clause reaches the caller
	as itself rather than wrapped in a second message.
"""

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }, { id: t:3 }]"

[[test.results]]
error = "Found 1.5f but the LIMIT clause must evaluate to a positive integer"

[[test.results]]
error = "Found 1dec but the LIMIT clause must evaluate to a positive integer"

[[test.results]]
error = "Found 1.5f but the START clause must evaluate to a positive integer"

[[test.results]]
error = "Found NONE but the LIMIT clause must evaluate to a positive integer"

[[test.results]]
error = "Found NULL but the LIMIT clause must evaluate to a positive integer"

[[test.results]]
error = "Found NONE but the START clause must evaluate to a positive integer"

[[test.results]]
error = "Found 5000000000 but the LIMIT clause must evaluate to a positive integer"

[[test.results]]
error = "Found 5000000000 but the START clause must evaluate to a positive integer"

[[test.results]]
error = "Found -1 but the LIMIT clause must evaluate to a positive integer"

[[test.results]]
error = "Found 2 but the LIMIT clause must evaluate to a positive integer"

[[test.results]]
error = "Found 1.5f but the LIMIT clause must evaluate to a positive integer"

[[test.results]]
error = "Found 1.5f but the LIMIT clause must evaluate to a positive integer"

[[test.results]]
error = "Found 1.5f but the LIMIT clause must evaluate to a positive integer"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[{ id: t:2 }]"
*/

CREATE t:1, t:2, t:3;

-- A count is not rounded.
SELECT id FROM t LIMIT 1.5;
SELECT id FROM t LIMIT 1dec;
SELECT id FROM t START 1.5;

-- Nor is an absent value read as zero.
SELECT id FROM t LIMIT NONE;
SELECT id FROM t LIMIT NULL;
SELECT id FROM t START NONE;

-- Nor is a count that does not fit.
SELECT id FROM t LIMIT 5000000000;
SELECT id FROM t START 5000000000;

-- A negative count and a non-numeric one name no number of rows.
SELECT id FROM t LIMIT -1;
SELECT id FROM t LIMIT "2";

-- The same holds where the paging is not pushed into the scan: over a
-- literal array, beneath a filter, and beneath a sort.
SELECT id FROM [1, 2, 3] LIMIT 1.5;
SELECT id FROM t WHERE k = 1 LIMIT 1.5;
SELECT id FROM t ORDER BY id LIMIT 1.5;

-- An error from evaluating the clause is the error the caller sees.
SELECT id FROM t LIMIT type::int("x");

-- A count that is written down as one still pages.
SELECT id FROM t ORDER BY id START 1 LIMIT 1;
