/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A RETURN inside a block statement ends that block with its value and nothing more: inside a transaction, the statements after the block still run and each reports its own result."
issue = 4389

[[test.results]]
value = "NONE"

[[test.results]]
value = "User:1"

[[test.results]]
value = "8"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "9"

[[test.results]]
value = "8"

[[test.results]]
value = "NONE"
*/

BEGIN TRANSACTION;
{
	LET $created = (CREATE ONLY User:1 SET name = 'Ann' RETURN id AS sid);
	RETURN $created.sid;
};
RETURN 8;
COMMIT TRANSACTION;

BEGIN TRANSACTION;
{ RETURN 9 };
{ RETURN 8 };
COMMIT TRANSACTION;
