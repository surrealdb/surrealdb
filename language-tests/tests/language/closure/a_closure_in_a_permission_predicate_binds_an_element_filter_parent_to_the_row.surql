/**
[env]
namespace = true
database = true
imports = ["language/closure/a_closure_in_a_permission_predicate_binds_an_element_filter_parent_to_the_row_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }

[test]
reason = "An element filter in a closure that a record user's `PERMISSIONS FOR select WHERE` predicate holds binds `$parent` to the row, nearer than a closure argument named `parent`: the table with the argument and the table with the `LET $parent` twin admit the same row. Runs as a record user."

[[test.results]]
value = "[pt_parent:2]"

[[test.results]]
value = "[pt_parent_cf:2]"
*/
SELECT VALUE id FROM pt_parent ORDER BY id;
SELECT VALUE id FROM pt_parent_cf ORDER BY id;
