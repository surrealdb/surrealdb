/**
[env]
imports = ["access/record/signup_import.surql"]

auth = { namespace = "test", database = "test", access = "account", rid = "user:bla" }

[test]

[[test.results]]
value = "[{ id: foo:baq, published: false, user: user:bla }, { id: foo:bar, published: true, user: user:other }]"

*/
SELECT * FROM foo;
