/**
[env]
imports = ["reproductions/a_record_user_of_an_undefined_table_reads_as_none_through_auth_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:b" }

[test]
reason = """A record user signed in with a record of a table that is not defined reads every field of `$auth` as `NONE`, in a permission and in a query alike: the record is absent, so no field is, and a permission comparing a field to one of them is satisfied only by a `NONE`. `$auth.id` is the exception, being the signed-in id itself, answered from the session without reading the record, so a permission over it admits the user's own rows. The exemption is the signed-in record's own: a record id naming an undefined table anywhere else, another record of the user's own table included, and the signed-in record read as a scan source, still fails with `TbNotFound`. The signed-in record is recognised from the session, and `$auth` is protected, so no binding can rebind it."""

[[test.results]]
value = "[{ author: user:b, id: post:1 }]"

[[test.results]]
value = "[{ id: post2:1 }]"

[[test.results]]
value = "[{ a: 1, id: post3:1 }]"

[[test.results]]
value = "user:b"

[[test.results]]
value = "user:b"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ email: NONE, id: user:b }"

[[test.results]]
value = "'dflt'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[{ e: NONE }]"

[[test.results]]
value = "[{ author: NONE, id: post:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'user' does not exist"

[[test.results]]
error = "The table 'user' does not exist"

[[test.results]]
error = "The table 'user' does not exist"

[[test.results]]
error = "The table 'user' does not exist"

[[test.results]]
value = "[user:b, { email: NONE, id: user:b }]"

*/

SELECT * FROM post;
SELECT * FROM post2;
SELECT * FROM post3;
RETURN $auth;
RETURN $auth.id;
RETURN $auth.email;
RETURN $auth.a.b;
RETURN $auth.*;
RETURN $auth.{id, email};
RETURN $auth.email ?? 'dflt';
RETURN $auth.tags[0];
RETURN [$auth.email];
SELECT $auth.email AS e FROM post;
SELECT * FROM post FETCH author;
LET $a = $auth; RETURN $a.email;
RETURN missing:1.email;
RETURN user:c.email;
RETURN user:c.id;
SELECT * FROM user:c;
SELECT * FROM user:b;
FOR $x IN [1] { RETURN [$session.rd.id, $session.rd.{ id, email }]; };
