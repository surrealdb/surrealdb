/**
[test]
issue = 7460
reason = """
Setup for `7460_a_select_permission_maps_dot_star_over_set_elements`: two
tables whose SELECT permission reads `ss.*[0]` over the same set,
`{[1, 2], [3]}`. Read per element, `ss.*[0]` is `{1, 3}`, so `denied`'s
permission is false for its row and `granted`'s is true.
"""
run = false
*/
DEFINE TABLE denied SCHEMALESS PERMISSIONS FOR select WHERE ss.*[0] = [1, 2];
DEFINE TABLE granted SCHEMALESS PERMISSIONS FOR select WHERE ss.*[0] CONTAINS 3;
CREATE denied:1 SET ss = <set>[[1, 2], [3]];
CREATE granted:1 SET ss = <set>[[1, 2], [3]];
