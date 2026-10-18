/**
[env]
namespace = true
database = true

[test]
reason = """
	A count of an indexed table in value position, correlated with the enclosing row through an
	equality or a membership, over every kind of key the enclosing row can hold. The count of the
	statement is the number of records whose field equals the key, so a key of any kind counts what
	a filter over the records counts: numbers of another representation that are the same number
	(`3`, `3.0f` and `3dec`), a float and a decimal of the same digits that are two numbers (`0.1f`
	and `0.1dec`, each counting only itself), a string, an array, a record id, `NULL`, a missing
	field and an enclosing row that is a record id.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: [{ count: 4 }], id: small:1 }, { c: [{ count: 4 }], id: small:2 }, { c: [{ count: 4 }], id: small:3 }, { c: [{ count: 1 }], id: small:4 }, { c: [{ count: 1 }], id: small:5 }, { c: [{ count: 1 }], id: small:6 }, { c: [{ count: 1 }], id: small:7 }, { c: [{ count: 1 }], id: small:8 }, { c: [{ count: 1 }], id: small:9 }, { c: [{ count: 1 }], id: small:10 }, { c: [{ count: 1 }], id: small:11 }, { c: [{ count: 1 }], id: small:12 }, { c: [{ count: 1 }], id: small:13 }, { c: [{ count: 4 }], id: small:14 }, { c: [{ count: 2 }], id: small:15 }, { c: [{ count: 2 }], id: small:16 }, { c: [{ count: 1 }], id: small:17 }, { c: [{ count: 2 }], id: small:18 }, { c: [{ count: 2 }], id: small:19 }]"

[[test.results]]
value = "[{ c: 4, id: small:1 }, { c: 4, id: small:2 }, { c: 4, id: small:3 }, { c: 1, id: small:4 }, { c: 1, id: small:5 }, { c: 1, id: small:6 }, { c: 1, id: small:7 }, { c: 1, id: small:8 }, { c: 1, id: small:9 }, { c: 1, id: small:10 }, { c: 1, id: small:11 }, { c: 1, id: small:12 }, { c: 1, id: small:13 }, { c: 4, id: small:14 }, { c: 2, id: small:15 }, { c: 2, id: small:16 }, { c: 1, id: small:17 }, { c: 2, id: small:18 }, { c: 2, id: small:19 }]"

[[test.results]]
value = "[{ c: [{ count: 4 }], id: small:1 }, { c: [{ count: 4 }], id: small:2 }, { c: [{ count: 4 }], id: small:3 }, { c: [{ count: 1 }], id: small:4 }, { c: [{ count: 1 }], id: small:5 }, { c: [{ count: 1 }], id: small:6 }, { c: [{ count: 1 }], id: small:7 }, { c: [{ count: 1 }], id: small:8 }, { c: [{ count: 1 }], id: small:9 }, { c: [{ count: 1 }], id: small:10 }, { c: [{ count: 1 }], id: small:11 }, { c: [{ count: 1 }], id: small:12 }, { c: [{ count: 1 }], id: small:13 }, { c: [{ count: 4 }], id: small:14 }, { c: [{ count: 2 }], id: small:15 }, { c: [{ count: 2 }], id: small:16 }, { c: [{ count: 1 }], id: small:17 }, { c: [{ count: 2 }], id: small:18 }, { c: [{ count: 2 }], id: small:19 }]"

[[test.results]]
value = "[{ c: [{ count: 4 }], id: small:1 }, { c: [{ count: 1 }], id: small:5 }, { c: [{ count: 1 }], id: small:11 }, { c: [{ count: 2 }], id: small:18 }]"

[[test.results]]
value = "[{ c: [{ count: 0 }], id: multi:1 }, { c: [{ count: 0 }], id: multi:2 }, { c: [{ count: 4 }], id: multi:3 }, { c: [{ count: 0 }], id: multi:4 }, { c: [{ count: 7 }], id: multi:5 }, { c: [{ count: 2 }], id: multi:6 }, { c: [{ count: 1 }], id: multi:7 }, { c: [{ count: 0 }], id: multi:8 }, { c: [{ count: 2 }], id: multi:9 }]"

[[test.results]]
value = "[{ c: [{ count: 6 }], id: small:1 }, { c: [{ count: 6 }], id: small:2 }, { c: [{ count: 6 }], id: small:3 }, { c: [{ count: 3 }], id: small:4 }, { c: [{ count: 3 }], id: small:5 }, { c: [{ count: 3 }], id: small:6 }, { c: [{ count: 3 }], id: small:7 }, { c: [{ count: 3 }], id: small:8 }, { c: [{ count: 3 }], id: small:9 }, { c: [{ count: 3 }], id: small:10 }, { c: [{ count: 3 }], id: small:11 }, { c: [{ count: 3 }], id: small:12 }, { c: [{ count: 3 }], id: small:13 }, { c: [{ count: 6 }], id: small:14 }, { c: [{ count: 2 }], id: small:15 }, { c: [{ count: 2 }], id: small:16 }, { c: [{ count: 3 }], id: small:17 }, { c: [{ count: 4 }], id: small:18 }, { c: [{ count: 4 }], id: small:19 }]"

[[test.results]]
value = "[{ c: [{ count: 4 }], id: small:1 }, { c: [{ count: 4 }], id: small:2 }, { c: [{ count: 4 }], id: small:3 }, { c: [{ count: 5 }], id: small:4 }, { c: [{ count: 5 }], id: small:5 }, { c: [{ count: 5 }], id: small:6 }, { c: [{ count: 5 }], id: small:7 }, { c: [{ count: 5 }], id: small:8 }, { c: [{ count: 5 }], id: small:9 }, { c: [{ count: 5 }], id: small:10 }, { c: [{ count: 5 }], id: small:11 }, { c: [{ count: 5 }], id: small:12 }, { c: [{ count: 5 }], id: small:13 }, { c: [{ count: 4 }], id: small:14 }, { c: [{ count: 6 }], id: small:15 }, { c: [{ count: 6 }], id: small:16 }, { c: [{ count: 5 }], id: small:17 }, { c: [{ count: 6 }], id: small:18 }, { c: [{ count: 6 }], id: small:19 }]"

[[test.results]]
value = "[{ c: [{ count: 4 }], id: small:1 }, { c: [{ count: 4 }], id: small:2 }, { c: [{ count: 4 }], id: small:3 }, { c: [{ count: 5 }], id: small:4 }, { c: [{ count: 5 }], id: small:5 }, { c: [{ count: 5 }], id: small:6 }, { c: [{ count: 5 }], id: small:7 }, { c: [{ count: 5 }], id: small:8 }, { c: [{ count: 5 }], id: small:9 }, { c: [{ count: 5 }], id: small:10 }, { c: [{ count: 5 }], id: small:11 }, { c: [{ count: 5 }], id: small:12 }, { c: [{ count: 5 }], id: small:13 }, { c: [{ count: 4 }], id: small:14 }, { c: [{ count: 6 }], id: small:15 }, { c: [{ count: 6 }], id: small:16 }, { c: [{ count: 5 }], id: small:17 }, { c: [{ count: 6 }], id: small:18 }, { c: [{ count: 6 }], id: small:19 }]"

*/

DEFINE TABLE big SCHEMALESS;
DEFINE INDEX big_num ON big FIELDS num;
DEFINE TABLE small SCHEMALESS;
DEFINE TABLE multi SCHEMALESS;
FOR $r IN [
	{ i: 1, n: 3 },
	{ i: 2, n: 3.0f },
	{ i: 3, n: 3dec },
	{ i: 4, n: 3.5f },
	{ i: 5, n: 0.1f },
	{ i: 6, n: 0.1dec },
	{ i: 7, n: [3] },
	{ i: 8, n: '3' },
	{ i: 9, n: NULL },
	{ i: 10, n: true },
	{ i: 11 },
	{ i: 12, n: 9007199254740993 },
	{ i: 13, n: 9007199254740992.0f },
	{ i: 14, n: 3 },
	{ i: 15, n: -0.0f },
	{ i: 16, n: 0 },
	{ i: 17, n: NaN },
	{ i: 18, n: big:1 },
	{ i: 19, n: big:1 }
] {
	CREATE type::record('big', $r.i) SET num = $r.n;
	CREATE type::record('small', $r.i) SET num = $r.n;
};
FOR $r IN [
	{ i: 1, ns: [1, 1] },
	{ i: 2, ns: [] },
	{ i: 3, ns: [3, 3.0f] },
	{ i: 4, ns: 3 },
	{ i: 5, ns: [3, 0.1f, 0] },
	{ i: 6, ns: [0.1dec, '3'] },
	{ i: 7, ns: [[3]] },
	{ i: 8 },
	{ i: 9, ns: [NULL, true] }
] {
	CREATE type::record('multi', $r.i) SET ns = $r.ns;
};
SELECT id, (SELECT count() FROM big WHERE num = $parent.num GROUP ALL) AS c FROM small ORDER BY id;
SELECT id, (SELECT VALUE count() FROM big WHERE num = $parent.num GROUP ALL)[0] AS c FROM small ORDER BY id;
SELECT id, (SELECT count() FROM big WHERE $parent.num = num GROUP ALL) AS c FROM small ORDER BY id;
SELECT id, (SELECT count() FROM big WHERE num = $parent.num GROUP ALL) AS c FROM [small:1, small:5, small:11, small:18, small:99];
SELECT id, (SELECT count() FROM big WHERE num IN $parent.ns GROUP ALL) AS c FROM multi ORDER BY id;
SELECT id, (SELECT count() FROM big WHERE num IN [$parent.num, 0] GROUP ALL) AS c FROM small ORDER BY id;
SELECT id, (SELECT count() FROM big WHERE num IN [$parent.num, 3] GROUP ALL) AS c FROM small ORDER BY id;
SELECT id, (SELECT count() FROM big WHERE num = $parent.num OR num = 3 GROUP ALL) AS c FROM small ORDER BY id;
