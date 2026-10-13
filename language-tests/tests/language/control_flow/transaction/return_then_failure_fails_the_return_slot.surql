/**
[test]
reason = "A failure after a RETURN in a block fails the transaction (registers #8 and #20): the RETURN's slot reports the failed transaction, the statements after the failure the cancelled one, and the COMMIT fails"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "An error occurred: x"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
error = "The table 't' does not exist"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
RETURN 1;
THROW "x";
CREATE t:1;
COMMIT;
SELECT * FROM t;
