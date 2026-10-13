/**
[test]
reason = "A RETURN in a value block bound by LET ends that block and binds its value (register #8), so the LET's own slot is NONE and the block's later statements run. Needs the value-scope RETURN lowering of lane L9: passes only once it merges"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: w:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
LET $x = { RETURN 1 };
CREATE w:1;
COMMIT;
RETURN $x;
