/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A schema statement under an `IF` whose condition is read from data, or in
	a `FOR` body, changes the catalog only when it runs. A statement after it
	reads the definition the run left: the new one where the branch was taken
	or the loop ran, the old one where it was not. That holds for a function
	call, a read of a defined parameter and a computed field, whether the
	later statement is a statement of its own or a later statement of the same
	block.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "6"

[[test.results]]
value = "2"
*/

DEFINE FUNCTION fn::v() { RETURN 1 };
DEFINE PARAM $p VALUE 1;
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD c ON t COMPUTED 1;
CREATE t:1 SET flag = true RETURN NONE;

-- Taken: the call reads the new body.
IF (SELECT VALUE flag FROM ONLY t:1) { DEFINE FUNCTION OVERWRITE fn::v() { RETURN 2 } };
fn::v();

-- Not taken: the call reads the body as it was.
IF !(SELECT VALUE flag FROM ONLY t:1) { DEFINE FUNCTION OVERWRITE fn::v() { RETURN 3 } };
fn::v();

-- A loop that runs redefines the parameter on each iteration.
FOR $i IN [3, 4] { DEFINE PARAM OVERWRITE $p VALUE $i };
$p;

-- A loop over nothing runs no iteration.
FOR $i IN (SELECT VALUE id FROM t WHERE flag = false) { DEFINE PARAM OVERWRITE $p VALUE 9 };
$p;

-- A computed field redefined under a taken branch.
IF (SELECT VALUE flag FROM ONLY t:1) { DEFINE FIELD OVERWRITE c ON t COMPUTED 2 };
SELECT VALUE c FROM t;

-- The same inside one block.
{
	IF (SELECT VALUE flag FROM ONLY t:1) { DEFINE FIELD OVERWRITE c ON t COMPUTED 3 };
	RETURN SELECT VALUE c FROM t;
};
{
	FOR $i IN [5, 6] { DEFINE PARAM OVERWRITE $p VALUE $i };
	RETURN $p;
};
{
	IF !(SELECT VALUE flag FROM ONLY t:1) { REMOVE FUNCTION fn::v };
	RETURN fn::v();
};
