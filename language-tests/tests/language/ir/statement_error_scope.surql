/**
[env]
namespace = true
database = true

[test]
reason = """
A THROW, or a BREAK or CONTINUE outside a loop, is the error of its own top-level
statement only: every statement around it still runs and answers its own result, including a RETURN
after it.
"""

[[test.results]]
error = "An error occurred: x"

[[test.results]]
value = "[]"

[[test.results]]
value = "[a:1]"

[[test.results]]
error = "An error occurred: y"

[[test.results]]
value = "2"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "1"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "3"
*/

THROW 'x';
CREATE a:1 RETURN NONE;
SELECT VALUE id FROM a;
IF true { THROW 'y' };
RETURN 2;
{ BREAK };
RETURN 1;
IF true { CONTINUE };
RETURN 3;
