/**
[env]
imports = ["reproductions/7465_value_clauses_read_as_the_caller_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "A clause whose value the caller can read back — VALUE, DEFAULT, COMPUTED — reads other tables with the caller's own permissions, as does ASSERT, so it can never copy a record the caller cannot select into a field the caller can. Contrast a permission clause, which reads unchecked and hands the caller only its verdict."
issue = 7465

[[test.results]]
value = "[{ by_computed: NONE, checked: 1, id: note:one }]"

[[test.results]]
value = "[{ by_computed: NONE, checked: 1, id: note:one }]"

[[test.results]]
value = "[]"
*/

CREATE note:one SET checked = 1;
SELECT * FROM note:one;
SELECT * FROM vault;
