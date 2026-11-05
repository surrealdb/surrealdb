/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A `TIMEOUT` clause bounds the work a statement does, so it is read before
	that work starts: the input is not begun until the clause has produced a
	duration, and a clause that produces none fails the statement on its own.

	The clause is cast rather than coerced, so a duration written down as text
	names one, `NONE` names no bound at all and leaves the statement
	unbounded, and anything else — a number, a string that is not a duration,
	`NULL` — is reported as the cast that could not be made.
"""

[[test.results]]
value = "[{ id: t:1, k: 1 }]"

[[test.results]]
value = "[{ id: t:1, k: 1 }]"

[[test.results]]
value = "[{ id: t:1, k: 1 }]"

[[test.results]]
error = "The query was not executed because it exceeded the timeout: 0ns"

[[test.results]]
error = "The query was not executed because it exceeded the timeout: 0ns"

[[test.results]]
value = "[{ id: t:1, k: 1 }]"

[[test.results]]
error = "Could not cast into `duration` using input `NULL`"

[[test.results]]
error = "Could not cast into `duration` using input `'bad'`"

[[test.results]]
error = "Could not cast into `duration` using input `1`"

[[test.results]]
error = "Could not cast into `duration` using input `'bad'`"

*/

CREATE t:1 SET k = 1;

-- A duration written as text is one.
SELECT * FROM t TIMEOUT "1s";
SELECT * FROM t ORDER BY k TIMEOUT "1s";

-- Including one that leaves no time to do the work.
SELECT * FROM t TIMEOUT "0s";
SELECT * FROM t ORDER BY k TIMEOUT 0s;

-- `NONE` names no bound; `NULL` names no duration.
SELECT * FROM t TIMEOUT NONE;
SELECT * FROM t TIMEOUT NULL;

-- A value that is not a duration is reported as the cast it is, unwrapped,
-- wherever the clause sits in the plan.
SELECT * FROM t TIMEOUT "bad";
SELECT * FROM t TIMEOUT 1;
SELECT * FROM t ORDER BY k TIMEOUT "bad";
