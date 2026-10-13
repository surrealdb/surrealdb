/**
[env]

[test]
reason = "Pre-filtered vector search keeps a conjunct that names its field through `type::field` as a residual judged in the search, whichever side of the covered conjunct it stands. The nearest neighbours returned are the nearest rows that satisfy every conjunct: pts:2 is nearer than pts:3 but holds another `cat`, and pts:4 is nearer than pts:5 but holds another `tag`."
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ cat: 'x', id: pts:1, point: [10f], tag: 'a' }, { cat: 'y', id: pts:2, point: [20f], tag: 'a' }, { cat: 'x', id: pts:3, point: [30f], tag: 'a' }, { cat: 'x', id: pts:4, point: [40f], tag: 'b' }, { cat: 'x', id: pts:5, point: [50f], tag: 'a' }, { cat: 'x', id: pts:6, point: [60f], tag: 'a' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ distance: 10f, id: pts:1 }, { distance: 30f, id: pts:3 }]"

[[test.results]]
value = "[{ distance: 10f, id: pts:1 }, { distance: 30f, id: pts:3 }, { distance: 50f, id: pts:5 }]"
*/

DEFINE FIELD tag ON pts TYPE string;
DEFINE INDEX idx_tag ON pts FIELDS tag;
DEFINE INDEX hn_pt ON pts FIELDS point HNSW DIMENSION 1;
INSERT INTO pts [
	{ id: pts:1, point: [ 10f ], tag: 'a', cat: 'x' },
	{ id: pts:2, point: [ 20f ], tag: 'a', cat: 'y' },
	{ id: pts:3, point: [ 30f ], tag: 'a', cat: 'x' },
	{ id: pts:4, point: [ 40f ], tag: 'b', cat: 'x' },
	{ id: pts:5, point: [ 50f ], tag: 'a', cat: 'x' },
	{ id: pts:6, point: [ 60f ], tag: 'a', cat: 'x' }
];
LET $pt = [0f];
SELECT id, vector::distance::knn() AS distance FROM pts
	WHERE point <|2,40|> $pt AND tag = 'a' AND type::field('cat') = 'x'
	ORDER BY distance;
SELECT id, vector::distance::knn() AS distance FROM pts
	WHERE type::field('cat') = 'x' AND tag = 'a' AND point <|3,40|> $pt
	ORDER BY distance;
