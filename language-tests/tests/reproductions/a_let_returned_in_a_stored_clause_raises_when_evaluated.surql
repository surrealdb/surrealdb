/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
In a stored clause (an event's `WHEN` or `THEN`, a field's `VALUE`,
`DEFAULT` or `ASSERT`) a `LET` written where its value is read, as in
`RETURN (LET …)` or `LET $v = (LET …)`, raises when the clause is evaluated:
inside an event wrapped in the event's error, inside a field as it stands. A
`LET` that is a statement of the clause's block binds as usual. The same text
at the top level of a query, in a block or in a function body called from the
query returns NONE.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:5 }]"
*/

DEFINE TABLE u;
DEFINE EVENT e ON u THEN { RETURN (LET $q = 0) };
CREATE u:1;
REMOVE EVENT e ON u;
DEFINE EVENT e ON u THEN { LET $v = (LET $q = 0); };
CREATE u:2;
REMOVE EVENT e ON u;
DEFINE EVENT e ON u WHEN { RETURN (LET $q = 0) } OR true THEN { RETURN 1 };
CREATE u:3;
REMOVE EVENT e ON u;
DEFINE EVENT e ON u THEN { IF true { RETURN (LET $q = 0) } };
CREATE u:4;
REMOVE EVENT e ON u;
DEFINE EVENT e ON u THEN { (LET $q = 0) };
CREATE u:5;
DEFINE TABLE w;
DEFINE FIELD f ON w VALUE { RETURN (LET $q = 0) };
CREATE w:1;
DEFINE FIELD OVERWRITE f ON w DEFAULT { RETURN (LET $q = 0) };
CREATE w:2;
DEFINE FIELD OVERWRITE f ON w ASSERT { RETURN (LET $q = 0) } OR true;
CREATE w:3 SET f = 1;
{ RETURN (LET $q = 0) };
DEFINE FUNCTION fn::r() { RETURN (LET $q = 0) };
RETURN fn::r();
RETURN (LET $q = 0);
SELECT * FROM u, w;
