/**
[env]
namespace = true
database = true

[test]
reason = """
A BREAK or CONTINUE that escapes every loop inside BEGIN … COMMIT is the
statement's error, and an error inside a block cancels it: nothing commits,
and every statement of the block reports the failed transaction.
"""

[[test.results]]
value = "NONE"

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
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[]"
*/

DEFINE TABLE t;
BEGIN;
CREATE t:1;
IF true { CONTINUE };
CREATE t:2;
COMMIT;
SELECT VALUE id FROM t;
BEGIN;
CREATE t:3;
BREAK;
COMMIT;
SELECT VALUE id FROM t;
