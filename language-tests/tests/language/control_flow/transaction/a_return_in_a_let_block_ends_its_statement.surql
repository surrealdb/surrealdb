/**
[test]
reason = """
A LET's value is no context, and neither is a block: a RETURN in a block bound by a LET ends the
LET's statement with its value, and the name is not bound. Inside a transaction that statement is
a context of its own, so the statements after it run and the transaction commits.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "[{ id: w:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[env]
namespace = "test"
database = "test"


*/

BEGIN;
LET $x = { RETURN 1 };
CREATE w:1;
COMMIT;
RETURN $x;
