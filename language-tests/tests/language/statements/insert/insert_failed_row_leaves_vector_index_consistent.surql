/**
[env]
namespace = true
database = true

[test]
reason = """
A row that fails after writing to an HNSW index is rolled back to its save
point along with its index entries: a nearest-neighbour query afterwards finds
only the rows that survive. Covers an `INSERT IGNORE` row that conflicts on a
unique index after its vector was indexed, and an `ON DUPLICATE KEY UPDATE`
update that moves a vector and then conflicts, caught by an API handler so its
transaction commits. Indexes are written in name order, so the unique index is
named to sort after the vector index: each failing row writes its vector entry
before the unique index refuses it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pt:1 }, { id: pt:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "500"

[[test.results]]
value = "[{ id: pt:1, v: [0f, 0f] }, { id: pt:2, v: [10f, 10f] }]"

[[test.results]]
value = "[{ d: 1.4142135623730951f, id: pt:2 }]"

*/

DEFINE TABLE pt SCHEMALESS;
DEFINE INDEX pt_z_tag ON pt FIELDS tag UNIQUE;
DEFINE INDEX pt_vec ON pt FIELDS v HNSW DIMENSION 2 DIST EUCLIDEAN;
INSERT INTO pt [
	{ id: 1, v: [0f, 0f], tag: 'a' },
	{ id: 2, v: [10f, 10f], tag: 'b' }
] RETURN NONE;

-- An ignored row whose vector sits right next to the query point
INSERT IGNORE INTO pt [{ id: 3, v: [1f, 1f], tag: 'a' }] RETURN NONE;
LET $q = [1f, 1f];
SELECT id FROM pt WHERE v <|2,40|> $q ORDER BY id;

-- An update that moves pt:2 next to the query point, then conflicts on `tag`
DEFINE API '/move' FOR post THEN (INSERT INTO pt [{ id: 2, v: [10f, 10f], tag: 'b' }] ON DUPLICATE KEY UPDATE v = [1.1f, 1.1f], tag = 'a');
api::invoke('/move', { method: 'post' }).status;
SELECT id, v FROM pt ORDER BY id;
SELECT id, vector::distance::knn() AS d FROM pt WHERE v <|1,40|> [9f, 9f];
