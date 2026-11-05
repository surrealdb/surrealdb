/**
[test]
reason = "A RETURN directly inside a block is a value statement like any other (register #8): the statements after it run, and the block's COMMIT keeps their writes"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }]"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
RETURN 1;
CREATE t:1;
COMMIT;
SELECT * FROM t;
