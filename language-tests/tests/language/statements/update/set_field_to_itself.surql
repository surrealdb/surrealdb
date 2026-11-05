/**
[env]
imports = ["./set_field_to_itself_import.surql"]
auth = { namespace = "test", database = "test", access = "account", rid = "user:bla" }

[test]
reason = "SET a = a reads a through the caller's field permissions, so it is not a no-op: a caller who cannot select the field reads NONE, which the field's type then rejects"

[[test.results]]
error = "Couldn't coerce value for field `private` of `data:1`: Expected `string` but found `NONE`"

*/
UPDATE data:1 SET private = private;
