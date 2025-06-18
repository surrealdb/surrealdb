/**
[env]
auth = { database = "test", namespace = "test", access = "test", rid = "user:john" }
imports = ["language/statements/delete/permissions/no_select_import.surql"]

[test]

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

DELETE friends_with:1 RETURN BEFORE;
DELETE friends_with:2 RETURN BEFORE;
