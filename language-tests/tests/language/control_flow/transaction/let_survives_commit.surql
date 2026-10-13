/**
[test]
reason = "A request LET inside a block is kept by COMMIT (register #1): a new name stays bound and a rebinding keeps the block's value for the rest of the request"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 2]"

[env]
namespace = "test"
database = "test"

*/

LET $u = 0;
BEGIN;
LET $t = 1;
LET $u = 2;
COMMIT;
RETURN [$t, $u];
