/**
[env]
imports = ["language/closure/permissions_and_clauses_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = """
Closures invoked from stored clauses as a record user: a table's PERMISSIONS
predicate, a field's VALUE and ASSERT clauses and its PERMISSIONS predicate,
and an event's THEN clause, each of which invokes a closure it makes and one
it hands a builtin. The table's select permission is a predicate, so the
ASSERT failure redacts the rejected value.
"""

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "[{ id: doc:3, owner: person:bob, secret: 's', x: 40, y: 1 }]"

[[test.results]]
error = "Found [REDACTED] for field `y`, with record `doc:4`, but field must conform to: (|$v: any| $v > 0)($value)"

[[test.results]]
value = "[[doc:1, 10, 's'], [doc:3, 40, 's']]"

[[test.results]]
value = "[11, 21, 41]"
*/

SELECT VALUE id FROM doc ORDER BY id;
CREATE doc:3 SET owner = person:bob, x = 4, y = 1;
CREATE doc:4 SET owner = person:bob, x = 1, y = -1;
SELECT VALUE [id, x, secret] FROM doc ORDER BY id;
SELECT VALUE v FROM log ORDER BY v;
