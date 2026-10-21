/**
[env]
imports = ["reproductions/7464_auth_id_when_the_id_field_is_hidden_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "`$auth.id` is the signed-in id whatever the `id` field's own select permission, in a path and in a destructure alike; the record's other fields are read as the table allows."
issue = 7464

[[test.results]]
value = "person:bob"

[[test.results]]
value = "{ id: person:bob, name: 'bob' }"

*/

RETURN $auth.id;
RETURN $auth.{ id, name };
