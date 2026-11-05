/**
[env]
imports = ["reproductions/a_computed_field_reading_its_own_table_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "A record user's read of a COMPUTED field whose body reads its own table derives the field once per record: the body's read names `id` alone and derives nothing."

[[test.results]]
value = "[ring:1, ring:2]"

[[test.results]]
value = "[{ id: ring:1, members: 2 }, { id: ring:2, members: 2 }]"

[[test.results]]
value = "[2]"

*/

SELECT VALUE id FROM ring ORDER BY id;
SELECT * FROM ring ORDER BY id;
SELECT VALUE members FROM ring:1;
