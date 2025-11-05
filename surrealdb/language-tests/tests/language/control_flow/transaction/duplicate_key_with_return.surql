/**
[test]
reason = "Test transaction failure with duplicate key and RETURN statement"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Database record `person:tobie` already exists"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
CREATE person:tobie;
CREATE person:jaime;
CREATE person:tobie;
RETURN { tobie: person:tobie, jaime: person:jaime };
COMMIT;

