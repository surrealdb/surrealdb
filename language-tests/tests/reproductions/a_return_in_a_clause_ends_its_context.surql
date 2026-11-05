/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A parenthesised RETURN follows one rule wherever it is written: it sets the value of the context
it stands in and ends it. The target of a write, LIMIT, START and the value of a LET are no
context, so a RETURN there ends the statement with its value before anything is written or bound,
as it does in an operand. A WHERE is a context per row, so a RETURN in a WHERE decides the row and
never ends the SELECT: the rows whose `a` is outside [1, 2] are selected.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "person:5"

[[test.results]]
value = "[person:5]"

[[test.results]]
value = "person:1"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "t:9"

[[test.results]]
value = "['person']"

[[test.results]]
value = "2"

[[test.results]]
value = "1"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "[1, 5]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[row:1, row:3]"

[[test.results]]
value = "[row:1, row:3]"
*/

CREATE person:1, person:5 RETURN NONE;
DELETE (RETURN person:5);
SELECT VALUE id FROM person:5;
UPDATE (RETURN person:1) SET z = 1;
SELECT VALUE z FROM person:1;
CREATE (RETURN t:9);
RETURN (INFO FOR DB).tables.keys();
SELECT * FROM person LIMIT (RETURN 2);
SELECT * FROM person START (RETURN 1);
LET $x = 1 + (RETURN 2);
RETURN $x;
RETURN 1 + (RETURN 2);
SELECT VALUE id FROM [{ id: 1, a: 0 }, { id: 2, a: 1 }, { id: 3, a: 1.5 }, { id: 4, a: 2 }, { id: 5, a: 3 }] WHERE { IF a > 2 { RETURN true }; IF a < 1 { RETURN true }; false };
CREATE row:1 SET a = 0 RETURN NONE;
CREATE row:2 SET a = 1.5 RETURN NONE;
CREATE row:3 SET a = 3 RETURN NONE;
SELECT VALUE id FROM row WHERE { IF a > 2 { RETURN true }; IF a < 1 { RETURN true }; false } ORDER BY id;
UPDATE row SET hit = true WHERE { IF a > 2 { RETURN true }; IF a < 1 { RETURN true }; false } RETURN VALUE id;
