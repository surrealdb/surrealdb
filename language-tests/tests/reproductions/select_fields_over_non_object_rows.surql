/**
[env]
namespace = true
database = true

[test]
reason = """
Fields beside no `*` over rows that are not objects. A field that only reads
the row (a name, or a rename of one) leaves a row that has no members as it is;
an object or a geometry becomes an object of the fields. A field that computes
a value, or writes an output path of more than one part, makes an object of the
fields from any row. `OMIT` is walked over the row that stands. `NONE` and
`NULL` sources yield no rows.
"""

[[test.results]]
value = "[{ id: person:1, n: 1, name: 'a' }]"

[[test.results]]
value = "[5]"

[[test.results]]
value = "['x']"

[[test.results]]
value = "[true]"

[[test.results]]
value = "[1.5f]"

[[test.results]]
value = "[d'2020-01-01T00:00:00Z']"

[[test.results]]
value = "[u'018ea0a8-5b50-7c3a-8e2c-000000000000']"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[5]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[1, { x: 2 }, 's']"

[[test.results]]
value = "[{ x: 1, z: NONE }, 5]"

[[test.results]]
value = "[{ x: 1 }]"

[[test.results]]
value = "[5]"

[[test.results]]
value = "[5]"

[[test.results]]
value = "5"

[[test.results]]
value = "[5]"

[[test.results]]
value = "[{ a: 1 }]"

[[test.results]]
value = "[{ a: 1, x: NONE }]"

[[test.results]]
value = "[{ x: { y: NONE } }]"

[[test.results]]
value = "[{ z: NONE }]"

[[test.results]]
value = "[{ x: NONE }]"

[[test.results]]
value = "[{ type: 'Point' }]"

[[test.results]]
value = "[{ m: 1, name: 'a' }, 5]"

[[test.results]]
value = "[{ name: 'a' }]"

*/
CREATE person:1 SET name = 'a', n = 1;
SELECT id, secret FROM 5;
SELECT x FROM 'x';
SELECT x FROM true;
SELECT x FROM 1.5;
SELECT x FROM d'2020-01-01T00:00:00Z';
SELECT x FROM u'018ea0a8-5b50-7c3a-8e2c-000000000000';
SELECT id, secret FROM NULL;
SELECT id, secret FROM NONE;
SELECT * FROM 5;
SELECT VALUE x FROM 5;
SELECT x FROM [1, { x: 2 }, 's'];
SELECT x, y AS z FROM [{ x: 1 }, 5];
SELECT x FROM { x: 1 };
SELECT x AS y FROM 5;
SELECT x FROM 5 WHERE true;
SELECT x FROM ONLY 5;
SELECT x OMIT y FROM [5];
SELECT 1 AS a FROM 5;
SELECT x, 1 AS a FROM [5];
SELECT x.y FROM 5;
SELECT x.y AS z FROM [5];
SELECT x FROM [(1, 2)];
SELECT type FROM [<geometry<point>>{ type: 'Point', coordinates: [1, 2] }];
SELECT name, n AS m FROM [person:1, 5];
SELECT name OMIT n FROM [person:1];
