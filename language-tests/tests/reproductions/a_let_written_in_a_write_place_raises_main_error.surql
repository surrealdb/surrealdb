/**
[env]
namespace = true
database = true

[test]
reason = """
A `LET` written as an expression inside a write's place, data clause,
condition or projection is refused with the error the operator tree raises
for it, wherever in the statement it is written.
"""

[[test.results]]
value = "[{ a: [{ x: 1 }, { x: 2 }], id: t:1 }]"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"
*/
CREATE t:1 SET a = [{ x: 1 }, { x: 2 }];
UPDATE t:1 SET a[(LET $z = 1)].x = 6;
UPDATE t:1 SET a = (LET $z = 1);
UPDATE t:1 SET a[WHERE (LET $z = 1)].x = 6;
INSERT INTO t (id, a[(LET $z = 0)]) VALUES (t:3, 1);
SELECT * FROM t WHERE (LET $z = 1);
SELECT (LET $z = 1) AS q FROM t;
UPDATE t:1 SET a[0].x = 9 RETURN a[(LET $z = 0)];
