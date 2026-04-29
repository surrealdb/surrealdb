/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Verify that THROW within a BEGIN/COMMIT transaction causes a full rollback"

[[test.results]]
value = "NONE"

[[test.results]]
error = "An error occurred: deliberate error"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"
*/

BEGIN;
THROW 'deliberate error';
CREATE person:one SET name = 'Alice';
COMMIT;
