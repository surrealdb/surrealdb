/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
`SELECT *, a AS b` projects every field of the row and, beside them, `b`
holding the row's `a`: the row's `a` stays. Each renamed field reads the row
as it was stored, so `SELECT *, a AS b, b AS c` gives `c` the stored `b`,
not the `a` written to `b` a moment earlier.
"""

[[test.results]]
value = "[{ a: 1, b: 2, id: t:1 }]"

[[test.results]]
value = "[{ a: 1, b: 1, id: t:1 }]"

[[test.results]]
value = "[{ a: 1, b: 2, id: t:1, n: 1 }]"

[[test.results]]
value = "[{ a: 1, b: 1, c: 2, id: t:1 }]"

[[test.results]]
value = "[{ a: 1, id: t:1, n: 1 }]"
*/
CREATE t:1 SET a = 1, b = 2;
SELECT *, a AS b FROM t;
SELECT *, a AS n FROM t;
SELECT *, a AS b, b AS c FROM t;
SELECT *, a AS n OMIT b FROM t;
