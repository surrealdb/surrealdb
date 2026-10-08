/**
[env]
namespace = true
database = true

[test]
reason = """
`*` beside fields over rows that are not objects. A field that only reads the
row (`type`, or a rename of one) leaves a row that is not an object as it is,
a geometry included. A field that computes a value writes onto an object made
of the row: a geometry as its `type` and `coordinates`, any other value as
nothing, so a scalar row yields an object of the computed fields alone. A
field written at a nested output path builds an object of the fields without
the row's own members. `OMIT` removes from whatever object results. Rows that
are objects keep their members and take the fields on top.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[(1f, 2f)]"

[[test.results]]
value = "[{ a: 1 }, { a: 1 }]"

[[test.results]]
value = "[(1f, 2f)]"

[[test.results]]
value = "[{ a: 1, coordinates: [1f, 2f], type: 'Point' }]"

[[test.results]]
value = "[{ c: NONE }]"

[[test.results]]
value = "[{ a: 1, x: 1 }, { a: 1 }]"

[[test.results]]
value = "[{ x: 1, y: 1 }, 7]"

[[test.results]]
value = "[{  }]"

[[test.results]]
value = "[{ a: { b: 1 } }]"

[[test.results]]
value = "[(1f, 2f)]"
*/
LET $g = <geometry<point>>{ type: 'Point', coordinates: [1, 2] };
SELECT *, type FROM $g;
SELECT *, 1 AS a FROM [7, 8];
SELECT * OMIT coordinates FROM $g;
SELECT *, 1 AS a FROM $g;
SELECT *, a.b AS c FROM [7];
SELECT *, 1 AS a FROM [{ x: 1 }, 7];
SELECT *, x AS y FROM [{ x: 1 }, 7];
SELECT *, 1 AS a OMIT a FROM [7];
SELECT *, 1 AS a.b FROM [7];
SELECT *, x AS y FROM $g;
