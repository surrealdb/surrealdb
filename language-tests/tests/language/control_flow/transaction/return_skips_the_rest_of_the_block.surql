/**
[test]
reason = "A RETURN inside a block skips the remaining statements up to the COMMIT, which get no result slot"

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
RETURN 1;
CREATE t:1;
COMMIT;
SELECT * FROM t;
