/**
[env]
imports = ["reproductions/a_computed_field_reading_its_own_table_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "A table permission that calls a function reading the permission's own table is judged with reads unchecked, so the function's read does not judge the permission again."

[[test.results]]
value = "[gated:1, gated:2]"

[[test.results]]
value = "[{ id: gated:1 }, { id: gated:2 }]"

[[test.results]]
value = "[gated:1]"

*/

SELECT VALUE id FROM gated ORDER BY id;
SELECT * FROM gated ORDER BY id;
SELECT VALUE id FROM gated:1;
