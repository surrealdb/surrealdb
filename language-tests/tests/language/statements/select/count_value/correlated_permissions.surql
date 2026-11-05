/**
[env]
imports = ["./correlated_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
	A count of a table read in value position and correlated with the row of the enclosing
	statement, as a record user: a table whose select permission reads the row counts the rows the
	user may read, a table that denies every read counts none, and a table nothing governs counts
	all.
"""

[[test.results]]
value = "[{ c: [{ count: 2 }], id: outer:1 }, { c: [{ count: 1 }], id: outer:2 }, { c: [{ count: 0 }], id: outer:3 }]"

[[test.results]]
value = "[{ c: [{ count: 1 }], id: outer:1 }, { c: [{ count: 1 }], id: outer:2 }, { c: [{ count: 0 }], id: outer:3 }]"

[[test.results]]
value = "[{ c: [{ count: 0 }], id: outer:1 }, { c: [{ count: 0 }], id: outer:2 }, { c: [{ count: 0 }], id: outer:3 }]"

[[test.results]]
value = "[{ c: 1, id: outer:1 }, { c: 1, id: outer:2 }, { c: 0, id: outer:3 }]"

[[test.results]]
value = "[outer:1, outer:2]"
*/

SELECT id, (SELECT count() FROM open WHERE num = $parent.num GROUP ALL) AS c FROM outer ORDER BY id;
SELECT id, (SELECT count() FROM owned WHERE num = $parent.num GROUP ALL) AS c FROM outer ORDER BY id;
SELECT id, (SELECT count() FROM denied WHERE num = $parent.num GROUP ALL) AS c FROM outer ORDER BY id;
SELECT id, (SELECT VALUE count() FROM owned WHERE num = $parent.num GROUP ALL)[0] AS c FROM outer ORDER BY id;
SELECT VALUE id FROM outer WHERE (SELECT count() FROM owned WHERE num = $parent.num GROUP ALL)[0].count > 0 ORDER BY id;
