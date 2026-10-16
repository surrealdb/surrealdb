/**
[env]
namespace = true
database = true

[test]
reason = """
	A count of a table read in value position and correlated with the row of the enclosing
	statement: the count each row owns, spelled with an equality, a range, a membership and no index,
	beside a `COUNT` index, inside a `WHERE`, and in the explained plan. The scalar `count` of a
	subquery is the twin that is not the count shape and answers the same numbers.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ c: [{ count: 2 }], id: small:1 }, { c: [{ count: 0 }], id: small:2 }, { c: [{ count: 0 }], id: small:3 }, { c: [{ count: 0 }], id: small:4 }]"

[[test.results]]
value = "[{ c: 2, id: small:1 }, { c: 0, id: small:2 }, { c: 0, id: small:3 }, { c: 0, id: small:4 }]"

[[test.results]]
value = "[{ c: [{ count: 3 }], id: small:1 }, { c: [{ count: 2 }], id: small:2 }, { c: [{ count: 3 }], id: small:3 }, { c: [{ count: 0 }], id: small:4 }]"

[[test.results]]
value = "[{ c: [{ count: 3 }], id: small:1 }, { c: [{ count: 3 }], id: small:2 }, { c: [{ count: 0 }], id: small:3 }, { c: [{ count: 5 }], id: small:4 }]"

[[test.results]]
value = "[{ c: [{ count: 5 }], id: small:1 }, { c: [{ count: 5 }], id: small:2 }, { c: [{ count: 5 }], id: small:3 }, { c: [{ count: 5 }], id: small:4 }]"

[[test.results]]
value = "[{ c: [{ count: 1 }], id: small:1 }, { c: [{ count: 0 }], id: small:2 }, { c: [{ count: 0 }], id: small:3 }, { c: [{ count: 0 }], id: small:4 }]"

[[test.results]]
value = "[{ c: 2, id: small:1 }, { c: 0, id: small:2 }, { c: 0, id: small:3 }, { c: 0, id: small:4 }]"

[[test.results]]
value = "[{ c: [{ count: 2 }], id: small:1 }, { c: [{ count: 0 }], id: small:2 }, { c: [{ count: 0 }], id: small:3 }, { c: [{ count: 0 }], id: small:4 }]"

[[test.results]]
value = "[{ c: [{ count: 2 }], id: small:1 }, { c: [{ count: 1 }], id: small:2 }, { c: [{ count: 0 }], id: small:3 }, { c: [{ count: 0 }], id: small:4 }]"

[[test.results]]
value = "[{ c: [{ count: 3 }], id: small:1 }, { c: [{ count: 3 }], id: small:2 }]"

[[test.results]]
value = "[small:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: small:1, o: { c: 2 } }, { id: small:2, o: { c: 0 } }, { id: small:3, o: { c: 0 } }, { id: small:4, o: { c: 0 } }]"

[[test.results]]
value = "[{ c: 2, id: small:1 }, { c: 0, id: small:2 }, { c: 0, id: small:3 }, { c: 0, id: small:4 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id, c]
    TableScan [ctx: Db] [table: small, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id, c]
    TableScan [ctx: Db] [table: small, direction: Forward]
''''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c: [{ count: 1 }], id: small:1 }, { c: [{ count: 0 }], id: small:2 }, { c: [{ count: 1 }], id: small:3 }, { c: [{ count: 0 }], id: small:4 }]"
*/

{
	DEFINE TABLE small SCHEMALESS;
	DEFINE TABLE big SCHEMALESS;
	DEFINE FIELD num ON big TYPE int;
	DEFINE FIELD tag ON big TYPE string;
	DEFINE INDEX big_num ON big FIELDS num;
	CREATE small:1 SET num = 1, tag = 'a' RETURN NONE;
	CREATE small:2 SET num = 2, tag = 'b' RETURN NONE;
	CREATE small:3 SET num = 7, tag = 'a' RETURN NONE;
	CREATE small:4 SET tag = 'z' RETURN NONE;
	CREATE big:1 SET num = 1, tag = 'a' RETURN NONE;
	CREATE big:2 SET num = 1, tag = 'b' RETURN NONE;
	CREATE big:3 SET num = 3, tag = 'a' RETURN NONE;
	CREATE big:4 SET num = 3, tag = 'a' RETURN NONE;
	CREATE big:5 SET num = 3, tag = 'b' RETURN NONE;
	DEFINE TABLE counted SCHEMALESS;
	DEFINE FIELD num ON counted TYPE int;
	DEFINE INDEX counted_count ON counted COUNT;
	DEFINE INDEX counted_num ON counted FIELDS num;
	CREATE counted:1 SET num = 1 RETURN NONE;
	CREATE counted:2 SET num = 1 RETURN NONE;
	CREATE counted:3 SET num = 2 RETURN NONE;
	RETURN 'OK';
};

-- An equality over an indexed field, an unindexed one, a range and a membership.
SELECT id, (SELECT count() FROM big WHERE num = $parent.num GROUP ALL) AS c FROM small ORDER BY id;
SELECT id, (SELECT VALUE count() FROM big WHERE num = $parent.num GROUP ALL)[0] AS c FROM small ORDER BY id;
SELECT id, (SELECT count() FROM big WHERE tag = $parent.tag GROUP ALL) AS c FROM small ORDER BY id;
SELECT id, (SELECT count() FROM big WHERE num > $parent.num GROUP ALL) AS c FROM small ORDER BY id;
SELECT id, (SELECT count() FROM big WHERE num IN [1, 3] GROUP ALL) AS c FROM small ORDER BY id;
SELECT id, (SELECT count() FROM big WHERE num = $parent.num AND tag = 'a' GROUP ALL) AS c FROM small ORDER BY id;
SELECT id, (SELECT count() FROM big WHERE num = $parent.num GROUP ALL)[0].count AS c FROM small ORDER BY id;
SELECT id, (SELECT count() FROM big WHERE num = $parent.num GROUP ALL LIMIT 1) AS c FROM small ORDER BY id;

-- A table with a `COUNT` index, and one counted whole.
SELECT id, (SELECT count() FROM counted WHERE num = $parent.num GROUP ALL) AS c FROM small ORDER BY id;
SELECT id, (SELECT count() FROM counted GROUP ALL) AS c FROM small ORDER BY id LIMIT 2;

-- In a `WHERE` and in a projection of a nested value.
SELECT VALUE id FROM small WHERE (SELECT count() FROM big WHERE num = $parent.num GROUP ALL)[0].count > 1 ORDER BY id;
SELECT VALUE id FROM small WHERE (SELECT count() FROM big WHERE num = $parent.num GROUP ALL)[0].count = NONE ORDER BY id;
SELECT id, { c: (SELECT VALUE count() FROM big WHERE num = $parent.num GROUP ALL)[0] } AS o FROM small ORDER BY id;

-- The scalar `count` of a subquery answers the same numbers.
SELECT id, count((SELECT id FROM big WHERE num = $parent.num)) AS c FROM small ORDER BY id;

-- As they are explained.
EXPLAIN SELECT id, (SELECT count() FROM big WHERE num = $parent.num GROUP ALL) AS c FROM small;
EXPLAIN SELECT id, (SELECT count() FROM big WHERE num > $parent.num GROUP ALL) AS c FROM small;

-- After the tables change.
DELETE big:1 RETURN NONE;
CREATE big:6 SET num = 7, tag = 'q' RETURN NONE;
SELECT id, (SELECT count() FROM big WHERE num = $parent.num GROUP ALL) AS c FROM small ORDER BY id;
