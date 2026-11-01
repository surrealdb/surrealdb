/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
ORDER BY accepts a function call, evaluated per row: type::field('a') orders the
rows by their field `a`, as ORDER BY a does.
"""
issue = 7296

[[test.results]]
value = "[]"

[[test.results]]
value = "[s:3, s:1, s:2]"

[[test.results]]
value = "[s:3, s:1, s:2]"
*/
INSERT INTO s [{ id: s:1, a: 2 }, { id: s:2, a: 3 }, { id: s:3, a: 1 }] RETURN NONE;
SELECT VALUE id FROM s ORDER BY type::field('a');
SELECT VALUE id FROM s ORDER BY a;
