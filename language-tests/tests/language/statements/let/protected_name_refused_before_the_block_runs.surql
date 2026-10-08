/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A LET of a protected name ($access, $auth, $token, $session) is refused wherever a block holds it — ahead of its first schema statement, in a loop body, in an IF branch, in a nested block, and after a schema statement — and the statement that holds it writes nothing"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "The table 'marker' does not exist"

[[test.results]]
error = "'token' is a protected variable and cannot be set"

[[test.results]]
error = "The table 'marker' does not exist"

[[test.results]]
error = "'access' is a protected variable and cannot be set"

[[test.results]]
error = "The table 'marker' does not exist"

[[test.results]]
error = "'session' is a protected variable and cannot be set"

[[test.results]]
error = "The table 'marker' does not exist"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "The table 'marker' does not exist"

*/

{
	CREATE marker:one;
	LET $auth = 1;
};
SELECT * FROM marker;

FOR $i IN [1, 2] {
	CREATE marker:two;
	LET $token = $i;
};
SELECT * FROM marker;

IF true {
	CREATE marker:three;
	LET $access = 'x';
} ELSE {
	NONE
};
SELECT * FROM marker;

{
	CREATE marker:four;
	LET $x = (SELECT VALUE id FROM marker);
	{ LET $session = $x; };
};
SELECT * FROM marker;

{
	DEFINE TABLE later;
	CREATE marker:five;
	LET $auth = 1;
};
SELECT * FROM marker;
