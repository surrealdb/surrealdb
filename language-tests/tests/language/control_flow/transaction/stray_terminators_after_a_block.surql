/**
[test]
reason = "A COMMIT or CANCEL with no open block is reported without affecting later statements"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot COMMIT without starting a transaction"

[[test.results]]
error = "Invalid statement: Cannot CANCEL without starting a transaction"

[[test.results]]
value = "1"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
COMMIT;
COMMIT;
CANCEL;
RETURN 1;
