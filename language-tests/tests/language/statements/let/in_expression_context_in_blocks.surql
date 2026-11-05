/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A LET written as an expression is refused wherever a statement holds it — in a SELECT's fields or WHERE, in a block's statement, a loop body, a LET value, and after a block's schema statement — with the error it raises at the top level, and the statement holding it writes nothing"

[[test.results]]
value = "[{ id: marker:keep }]"

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
value = "[marker:keep]"

*/

CREATE marker:keep;

SELECT [LET $x = 1] AS a FROM marker;
SELECT * FROM marker WHERE id = (LET $x = 1);

{
	CREATE marker:one;
	RETURN [LET $x = 1];
};

FOR $i IN [1, 2] {
	CREATE marker:two;
	{ a: LET $x = $i };
};

{
	LET $v = type::is_int(LET $y = 1);
	RETURN $v;
};

{
	DEFINE TABLE later;
	CREATE marker:three;
	RETURN [LET $x = 1];
};

SELECT VALUE id FROM marker ORDER BY id;
