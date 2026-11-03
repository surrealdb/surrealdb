/**
[env]
namespace = true
database = true

[test]
reason = "A type an event's WHEN or THEN, a field's permission, a table's permission or a function's permission names through a cast is not removed"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the event 'e' on the table 't' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the event 'e' on the table 't' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': an expression of the field 'status' on the table 't' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': a permission of the table 't' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the function 'fn::f' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TABLE t SCHEMALESS;
DEFINE EVENT e ON t WHEN <mood>$after.status = 'ok' THEN {};
REMOVE TYPE mood;
REMOVE EVENT e ON t;
DEFINE EVENT e ON t THEN { LET $m = <mood>$after.status; };
REMOVE TYPE mood;
REMOVE EVENT e ON t;
DEFINE FIELD status ON t PERMISSIONS FOR select WHERE <mood>status = 'ok';
REMOVE TYPE mood;
REMOVE FIELD status ON t;
DEFINE TABLE OVERWRITE t SCHEMALESS PERMISSIONS FOR select WHERE <mood>status = 'ok';
REMOVE TYPE mood;
DEFINE TABLE OVERWRITE t SCHEMALESS;
DEFINE FUNCTION fn::f() { RETURN 1; } PERMISSIONS WHERE <mood>'ok' = 'ok';
REMOVE TYPE mood;
REMOVE FUNCTION fn::f;
REMOVE TYPE mood;
