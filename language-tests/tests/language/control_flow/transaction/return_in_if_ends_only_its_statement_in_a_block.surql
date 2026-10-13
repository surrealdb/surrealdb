/**
[test]
reason = "A RETURN nested in a statement-level IF ends that IF statement and no more (register #8): the statements after it in the block run and commit"

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
IF true { RETURN 1 };
CREATE t:1;
COMMIT;
SELECT * FROM t;
