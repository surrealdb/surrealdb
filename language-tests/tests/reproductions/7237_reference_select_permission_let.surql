/**
[env]
namespace = true
database = true
imports = ["reproductions/7237_reference_select_permission_let_import.surql"]
auth = { namespace = "test", database = "test", access = "a7237_access", rid = "b7237:y" }

[test]
reason = "Record auth: bare SELECT * from a7237 works; LET binds subquery rows to $t; RETURN $t matches (#7237)"
issue = 7237

[[test.results]]
value = "[{ bs: [b7237:y], id: a7237:x }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ bs: [b7237:y], id: a7237:x }]"

*/

SELECT * FROM a7237;

LET $t = (SELECT * FROM a7237);

RETURN $t;
