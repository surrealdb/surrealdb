/**
[env]
namespace = true
database = true

[test]
reason = """
A geometry answers `type` and `coordinates` like an object, so a projection
that names fields reads them off one. `*` does not: the row is the geometry,
and `SELECT *`, `SELECT * OMIT …` and `SELECT *, type` all yield it as it
stands. Expanding it under `*` turned the row into an object of its two
members, which is a different row and a different value than the per-row form
returns.

A record id under `*` is the other way round — it is fetched, and the record
is the row.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[(1f, 2f)]"

[[test.results]]
value = "[{ type: 'Point' }]"

[[test.results]]
value = "[{ type: 'Point', xy: [1f, 2f] }]"

[[test.results]]
value = "[(1f, 2f)]"

[[test.results]]
value = "[(1f, 2f)]"

[[test.results]]
value = "['Point']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "[7]"
*/

LET $g = <geometry<point>>{ type: 'Point', coordinates: [1, 2] };

-- `*` yields the geometry, whatever else stands beside it.
SELECT * FROM $g;

-- A projection naming fields reads them off the geometry.
SELECT type FROM $g;
-- Aliased, because an object of exactly `type` and `coordinates` is
-- itself read as a geometry when it is written down.
SELECT type, coordinates AS xy FROM $g;

SELECT * OMIT coordinates FROM $g;
SELECT *, type FROM $g;
SELECT VALUE type FROM $g;

-- A record id under `*` is fetched, and the record is the row.
DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET a = 1 RETURN NONE;
SELECT * FROM [t:1][0];
SELECT * OMIT a FROM [t:1][0];

-- Every other scalar passes through.
SELECT * FROM [7][0];
