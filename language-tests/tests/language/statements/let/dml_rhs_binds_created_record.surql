/**
[test]
reason = "Top-level `LET $r = CREATE ...` plans its DML right-hand side and binds the created record."

[env]

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

*/

LET $r = CREATE created:once SET v = 1;
RETURN $r[0].v;
