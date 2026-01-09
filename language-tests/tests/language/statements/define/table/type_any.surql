/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: thing:x3gpqiodnza5kamixpj0 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: thing:l4e3zk9wqybljokmpsji, in: foo:one, out: foo:two }]"
skip-record-id-key = true

*/

DEFINE TABLE thing TYPE ANY;
CREATE thing;
RELATE foo:one->thing->foo:two;
