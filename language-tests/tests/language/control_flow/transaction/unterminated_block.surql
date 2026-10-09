/**
[test]
reason = "A block with no COMMIT or CANCEL fails every statement inside it"

[[test.results]]
value = "[{ id: t:0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Missing COMMIT statement"

[[test.results]]
error = "Missing COMMIT statement"

[env]
namespace = "test"
database = "test"

*/

CREATE t:0;
BEGIN;
CREATE t:1;
RETURN 2;
