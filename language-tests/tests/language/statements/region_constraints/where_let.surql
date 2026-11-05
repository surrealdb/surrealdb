/**
[env]
namespace = true
database = true

[test]
reason = "A block in a WHERE clause is an ordinary block: its LET binds a name for the rest of the block, and the clause keeps the row when the block's value is true."

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[]"
*/
CREATE t:1;
SELECT VALUE id FROM t WHERE { LET $a = 1; $a == 1 };
SELECT VALUE id FROM t WHERE { LET $a = 1; $a == 2 };
