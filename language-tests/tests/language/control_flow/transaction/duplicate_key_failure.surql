/**
[test]
reason = "Test transaction failure due to duplicate key"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Database record `person:tobie` already exists"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
CREATE person:tobie;
CREATE person:jaime;
CREATE person:tobie;
COMMIT;

