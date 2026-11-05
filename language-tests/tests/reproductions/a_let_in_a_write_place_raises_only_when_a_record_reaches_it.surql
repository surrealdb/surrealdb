/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A `LET` written as an expression inside a write's place raises the operator
tree's refusal when the place is evaluated for a record. A place no record
reaches raises nothing. Inside an event the refusal is raised as the event
runs, wrapped in the event's error.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: [{ x: 1 }], id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:1 }]"

[[test.results]]
value = "[{ a: [{ x: 1 }], id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
value = "[{ a: [{ x: 1 }], id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:4 }]"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
value = "[]"
*/

DEFINE TABLE t;
DEFINE TABLE u;
CREATE t:1 SET a = [{ x: 1 }];

DEFINE EVENT e ON u WHEN true THEN { UPDATE t:99 SET a[(LET $q = 0)].x = 9 };
CREATE u:1;
SELECT * FROM t;
REMOVE EVENT e ON u;

DEFINE EVENT e ON u WHEN true THEN { UPDATE t:1 SET a[(LET $q = 0)].x = 9 };
CREATE u:2;
SELECT * FROM t;
REMOVE EVENT e ON u;

DEFINE EVENT e ON u WHEN true THEN { (LET $q = 0) };
CREATE u:4;
UPDATE t:1 SET a[(LET $z = 0)].x = 6;
UPDATE t:99 SET a[(LET $z = 0)].x = 6;
