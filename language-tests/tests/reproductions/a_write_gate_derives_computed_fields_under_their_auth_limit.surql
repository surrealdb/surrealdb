/**
[env]
imports = ["reproductions/a_write_gate_derives_computed_fields_under_their_auth_limit_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "A write's permission reading a COMPUTED field whose body calls a user-defined function or holds a paged statement, one over a table the field's principal cannot read included, derives the field under its AUTH LIMIT for a record user: UPDATE, CREATE and DELETE run, and a record the derived value denies is not written."

[[test.results]]
value = "[{ id: doc:pub, n: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: doc:new2 }]"

[[test.results]]
value = "[NONE, NONE]"

[[test.results]]
value = "[doc:sec, doc:tg]"

*/
UPDATE doc SET n = 1 RETURN id, n;
UPDATE doc:sec SET n = 2 RETURN id;
CREATE doc:new SET status = 'secret' RETURN id;
CREATE doc:new2 SET status = 'public' RETURN id;
DELETE doc RETURN VALUE id;
SELECT VALUE id FROM doc ORDER BY id;
