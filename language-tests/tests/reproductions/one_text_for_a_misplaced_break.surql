/**
[env]
namespace = true
database = true

[test]
reason = """
A BREAK or CONTINUE with no FOR around it has no loop to act on, and raises one
text, the misplaced-control-flow error, wherever it is written: as a statement,
in a block or an IF, in a source, a WHERE or a SET or CONTENT value, in a
function body, a closure body, a field's VALUE and an event's THEN, and in a
computed field, which a read derives as a projection layer's column. An event's
error carries the event's name. The error is that statement's own: the
statements around it still run, and no write the statement had begun is kept.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[a:1]"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "[a:1]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event ev: Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "[]"

[[test.results]]
value = "[c1:1]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."
*/

DEFINE TABLE a SCHEMALESS;
CREATE a:1 SET k = 1 RETURN VALUE id;
-- With no loop around it, a BREAK or CONTINUE is the error of its own statement, whatever position it is written in.
BREAK;
CONTINUE;
{ BREAK };
{ CONTINUE };
IF true { BREAK };
SELECT * FROM (BREAK);
SELECT * FROM a WHERE { BREAK };
SELECT * FROM a WHERE { CONTINUE };
UPDATE a SET k = { BREAK };
UPDATE a SET k = 2 WHERE { CONTINUE };
CREATE a:2 CONTENT { k: { BREAK } };
-- None of the writes above is kept.
SELECT VALUE id FROM a;
SELECT VALUE k FROM a;
-- A frame the signal cannot leave raises the same text: a function body, a closure body, a field's VALUE and an event's THEN.
DEFINE FUNCTION fn::brk() { BREAK };
RETURN fn::brk();
LET $f = || { CONTINUE };
RETURN $f();
DEFINE TABLE v1 SCHEMALESS;
DEFINE FIELD n ON v1 VALUE { BREAK };
CREATE v1:1 SET n = 1;
SELECT VALUE id FROM v1;
DEFINE TABLE e1 SCHEMALESS;
DEFINE EVENT ev ON e1 THEN { BREAK };
CREATE e1:1;
SELECT VALUE id FROM e1;
-- A computed field is a projection layer's column: it raises the same text where a read derives it.
CREATE c1:1 SET k = 1 RETURN VALUE id;
DEFINE FIELD c ON c1 COMPUTED { CONTINUE };
SELECT * FROM c1:1;
SELECT * FROM c1;
