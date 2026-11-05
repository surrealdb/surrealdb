/**
[env]
imports = ["reproductions/7460_a_select_permission_maps_dot_star_over_set_elements_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "users:reader" }

[test]
issue = 7460
reason = """
A table's SELECT permission reads the parts after `.*` over a set per element,
as every other expression does (see
`7460_parts_after_dot_star_on_a_set_apply_to_each_element`). Over
`{[1, 2], [3]}`, `ss.*[0]` is `{1, 3}`, so on every engine a record user is
denied the row guarded by `ss.*[0] = [1, 2]` and granted the row guarded by
`ss.*[0] CONTAINS 3`.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[granted:1]"
*/
SELECT VALUE id FROM denied;
SELECT VALUE id FROM granted;
