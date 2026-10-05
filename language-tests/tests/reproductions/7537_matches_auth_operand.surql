/**
[env]
imports = ["reproductions/7537_matches_auth_operand_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }
planner-strategy = ["all-ro"]

[test]
reason = """For a record user `$auth` is the user's record id, so `$auth.principals` reads a \
record under the table's SELECT permission. That read is not resolved at plan time, so a \
MATCHES whose query depends on it names no index and is refused — on its own, next to a KNN \
operator, and next to another MATCHES, where it is never dropped. The same query bound first \
with LET is a literal and is matched through the index."""
issue = 7537

[[test.results]]
error = "There was no suitable index supporting the expression: acl @2,OR@ array::join(...)"

[[test.results]]
error = "There was no suitable index supporting the expression: acl @2,OR@ array::join(...)"

[[test.results]]
error = "There was no suitable index supporting the expression: acl @2,OR@ array::join(...)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[doc:2]"
*/

SELECT VALUE id FROM doc WHERE acl @2,OR@ array::join($auth.principals, ' ');
SELECT VALUE id FROM doc WHERE e <|2, 40|> [1.0, 0.0] AND acl @2,OR@ array::join($auth.principals, ' ');
SELECT VALUE id FROM doc WHERE text @1@ 'hello' AND acl @2,OR@ array::join($auth.principals, ' ');
LET $q = array::join($auth.principals, ' ');
SELECT VALUE id FROM doc WHERE text @1@ 'hello' AND acl @2,OR@ $q;
SELECT VALUE id FROM doc WHERE e <|2, 40|> [1.0, 0.0] AND acl @2,OR@ $q;
