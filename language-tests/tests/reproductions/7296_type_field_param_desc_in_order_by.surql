/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
ORDER BY accepts a function call with a direction: type::field($f) DESC orders
the rows by the field the parameter names, descending, as ORDER BY a DESC does.
"""
issue = 7296

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[s:2, s:1, s:3]"

[[test.results]]
value = "[s:2, s:1, s:3]"
*/
INSERT INTO s [{ id: s:1, a: 2 }, { id: s:2, a: 3 }, { id: s:3, a: 1 }] RETURN NONE;
LET $f = 'a';
SELECT VALUE id FROM s ORDER BY type::field($f) DESC;
SELECT VALUE id FROM s ORDER BY a DESC;
