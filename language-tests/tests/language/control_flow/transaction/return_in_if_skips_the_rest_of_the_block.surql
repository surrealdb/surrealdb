/**
[test]
reason = "A RETURN nested in an IF skips the remaining statements of the block like a top-level RETURN"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 't' does not exist"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
IF true { RETURN 1 };
CREATE t:1;
COMMIT;
SELECT * FROM t;
