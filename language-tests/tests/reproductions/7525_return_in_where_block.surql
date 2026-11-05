/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "RETURN inside a block in a WHERE clause ends that row's WHERE with its value: the first RETURN reached decides the row"
issue = 7525

[[test.results]]
value = "[{ a: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[t:2, t:3]"

[[test.results]]
value = "[t:3]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ after: true, r: [2] }"
*/

SELECT a FROM [{ a: 1 }, { a: 2 }] WHERE { return a > 1 };

-- Over a table: RETURN ends only the WHERE of its row, whatever reads the rows. In
-- `{ RETURN a > 1 } AND { RETURN a < 3 }` the first RETURN decides the row.
CREATE t:1 SET a = 1 RETURN NONE;
CREATE t:2 SET a = 2 RETURN NONE;
CREATE t:3 SET a = 3 RETURN NONE;
SELECT VALUE a FROM t WHERE { RETURN a > 1 } ORDER BY a;
SELECT VALUE a FROM t WHERE { LET $x = a; IF $x = 2 { RETURN true }; RETURN false };
SELECT VALUE a FROM t WHERE { RETURN a > 1 } ORDER BY a DESC LIMIT 1;
SELECT VALUE a FROM t WHERE { RETURN a > 1 } AND { RETURN a < 3 };
SELECT VALUE count() FROM t WHERE { RETURN a > 1 } GROUP ALL;
UPDATE t SET b = true WHERE { RETURN a > 1 } RETURN VALUE id;
DELETE t WHERE { RETURN a = 3 } RETURN VALUE $before.id;
SELECT VALUE a FROM t ORDER BY a;

-- Inside a function the statement still finishes, and so does the function.
DEFINE FUNCTION fn::filtered() { LET $r = SELECT VALUE a FROM t WHERE { RETURN a > 1 }; RETURN { r: $r, after: true }; };
RETURN fn::filtered();
