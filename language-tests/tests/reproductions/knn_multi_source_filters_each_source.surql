/**
[env]
namespace = true
database = true

[test]
reason = """
A KNN predicate over several FROM sources holds only for the rows a KNN
scan of the statement yields. A source with no KNN index of its own — a
table, or a record of such a table — contributes no row, whichever order
the sources are written in, and a missing table still raises its error.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:1, point: [1f, 2f, 3f, 4f] }, { id: pts:2, point: [4f, 5f, 6f, 7f] }, { id: pts:3, point: [8f, 9f, 10f, 11f] }]"

[[test.results]]
value = "[{ id: other:1, point: [1f, 2f, 3f, 4f] }, { id: other:2, point: [2f, 3f, 4f, 5f] }]"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }]"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }]"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }]"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }]"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }, { id: pts:1 }, { id: pts:2 }]"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }]"

[[test.results]]
error = "The table 'nosuch' does not exist"

*/
DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12;
INSERT INTO pts [
	{ id: pts:1, point: [1f, 2f, 3f, 4f] },
	{ id: pts:2, point: [4f, 5f, 6f, 7f] },
	{ id: pts:3, point: [8f, 9f, 10f, 11f] },
];
INSERT INTO other [
	{ id: other:1, point: [1f, 2f, 3f, 4f] },
	{ id: other:2, point: [2f, 3f, 4f, 5f] },
];
-- `other` holds the nearest vectors, but no KNN scan reads it.
SELECT id FROM pts, other WHERE point <|2,40|> [2f, 3f, 4f, 5f];
SELECT id FROM other, pts WHERE point <|2,40|> [2f, 3f, 4f, 5f];
SELECT id FROM pts, other:1 WHERE point <|2,40|> [2f, 3f, 4f, 5f];
-- A record no scan of the statement yields is not one of its rows.
SELECT id FROM pts:3, pts WHERE point <|2,40|> [2f, 3f, 4f, 5f];
SELECT id FROM pts, pts WHERE point <|2,40|> [2f, 3f, 4f, 5f];
-- A table a write earlier in the block creates, missing when the block is
-- planned, contributes no row either.
{
	INSERT INTO fresh [{ id: fresh:1, point: [2f, 3f, 4f, 5f] }];
	RETURN SELECT id FROM pts, fresh WHERE point <|2,40|> [2f, 3f, 4f, 5f];
};
SELECT id FROM pts, nosuch WHERE point <|2,40|> [2f, 3f, 4f, 5f];
