/**
[env]
namespace = true
database = true

[test]
reason = """
A BREAK, CONTINUE or RETURN that no enclosing construct catches is raised where
the statement holding it runs: the statements before it have run, a block's
writes are undone with it, and the statements after it still run.
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
value = "[{ id: t:4 }]"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "[{ id: t:5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:1, t:4, t:5]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:6 }]"

[[test.results]]
value = "'early'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:1, t:4, t:5, t:6]"
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
