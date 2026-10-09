/**
[test]
reason = "A BEGIN skipped after a RETURN does not abort the block"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
RETURN 1;
BEGIN;
COMMIT;
RETURN 2;
