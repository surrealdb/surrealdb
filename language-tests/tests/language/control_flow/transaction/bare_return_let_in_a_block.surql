/**
[test]
reason = "A bare RETURN as the value of a LET stands in no value scope, so it ends its top-level statement with its value inside a block as outside one (register #8): the LET binds nothing and the statements after it run"

[[test.results]]
value = "NONE"

[[test.results]]
value = "5"

[[test.results]]
value = "[{ id: w:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[w:1]"

[[test.results]]
value = "NONE"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
LET $x = RETURN 5;
CREATE w:1;
COMMIT;
SELECT VALUE id FROM w;
RETURN $x;
