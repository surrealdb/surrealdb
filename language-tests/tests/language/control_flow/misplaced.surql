/**
[env]
namespace = true
database = true

[test]
reason = """
A BREAK, CONTINUE or RETURN that no enclosing construct catches is raised where
the statement holding it runs: the statements before it have run, a block's
writes are undone with it, and the statements after it still run. Inside
BEGIN … COMMIT an escaping BREAK or CONTINUE is the statement's error, which
cancels the transaction as any other error there does: the statements before
it report the failed transaction, those after it the cancelled one, and the
COMMIT fails. A RETURN there is a value statement like any other: the
statements after it run and commit with the block.
"""

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:6 }]"

[[test.results]]
value = "'early'"

[[test.results]]
value = "[{ id: t:7 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:1, t:6, t:7]"
*/

CREATE t:1;
BREAK;
CONTINUE;
IF true { BREAK };
{ CREATE t:2; BREAK; CREATE t:3 };
SELECT VALUE id FROM t;
BEGIN;
CREATE t:4;
IF true { CONTINUE };
CREATE t:5;
COMMIT;
SELECT VALUE id FROM t;
BEGIN;
CREATE t:6;
RETURN 'early';
CREATE t:7;
COMMIT;
SELECT VALUE id FROM t;
