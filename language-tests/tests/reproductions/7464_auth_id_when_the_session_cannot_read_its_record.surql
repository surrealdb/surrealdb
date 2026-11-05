/**
[env]
imports = ["reproductions/7464_auth_id_when_the_session_cannot_read_its_record_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "`$auth.id` is the signed-in record's id, answered from the session without reading the record, so it holds where the caller cannot select its own record: in a statement, a VALUE, a COMPUTED and a permission clause alike. Every other field of `$auth` is a read of the record under the table's permissions, so `$auth.name` stays NONE, and a destructure reads each the same way. A FOR cannot bind the protected name `$auth`."
issue = 7464

[[test.results]]
value = "person:bob"

[[test.results]]
value = "person:bob"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'bob'"

[[test.results]]
value = "{ id: person:bob, name: NONE }"

[[test.results]]
value = "[{ id: doc:one, owner: person:bob, stamp: person:bob, viewer: person:bob }]"

[[test.results]]
value = "[{ id: doc:one, owner: person:bob, stamp: person:bob, viewer: person:bob }]"

[[test.results]]
value = "[]"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

*/

RETURN $auth;
RETURN $auth.id;
RETURN $auth.name;
RETURN record::id($auth);
RETURN $auth.{ id, name };
CREATE doc:one SET owner = person:bob;
SELECT * FROM doc;
DELETE doc:one;
FOR $auth IN [person:alice] { RETURN $auth.id; };
