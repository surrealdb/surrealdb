/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
ORDER BY accepts a function call: type::fields(['a', 'b']) orders the rows by the
array of their fields `a` and `b`, compared element by element.
"""
issue = 7296

[[test.results]]
value = "[]"

[[test.results]]
value = "[s:3, s:1, s:2]"
*/
INSERT INTO s [{ id: s:1, a: 1, b: 2 }, { id: s:2, a: 2, b: 0 }, { id: s:3, a: 1, b: 1 }] RETURN NONE;
SELECT VALUE id FROM s ORDER BY type::fields(['a', 'b']);
