/**
[env]
namespace = true
database = true
imports = ["reproductions/7237_reference_select_permission_let_import.surql"]
auth = { namespace = "test", database = "test", access = "a7237_access", rid = "b7237:y" }

[test]
reason = "Record auth: bare SELECT * from a7237 works; LET $t = (SELECT * FROM a7237) yields NONE (#7237 — permission/id binding differs vs inline SELECT)"
issue = 7237

[[test.results]]
value = "[{ bs: [b7237:y], id: a7237:x }]"

[[test.results]]
value = "NONE"

*/

SELECT * FROM a7237;

LET $t = (SELECT * FROM a7237);
