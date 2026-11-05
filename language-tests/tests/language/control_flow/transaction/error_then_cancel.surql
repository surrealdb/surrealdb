/**
[test]
reason = "A CANCEL after an error in a block answers NONE like a CANCEL of a healthy block (register #20): every statement of the request has a slot"

[[test.results]]
value = "NONE"

[[test.results]]
error = "An error occurred: x"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
THROW 'x';
CREATE t:1;
CANCEL;
RETURN 2;
