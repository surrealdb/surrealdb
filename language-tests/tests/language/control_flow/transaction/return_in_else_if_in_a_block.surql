/**
[test]
reason = "A RETURN in the ELSE IF arm of a statement-level IF ends only that IF statement (register #8): the statements after it in the block run and commit"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "[{ id: a:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[a:1]"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
IF false { 1 } ELSE IF true { RETURN 2 };
CREATE a:1;
COMMIT;
SELECT VALUE id FROM a;
