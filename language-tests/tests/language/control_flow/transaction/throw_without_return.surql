/**
[test]
reason = "Test transaction with THROW statement (no RETURN, with COMMIT)"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "An error occurred: there was an error"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
CREATE person:tobie;
CREATE person:jaime;
THROW 'there was an error';
COMMIT;

