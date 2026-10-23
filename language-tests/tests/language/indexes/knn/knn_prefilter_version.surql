/**
[env]
backend = ["mem", "surrealkv"]
versioned = true
timeout = 15000

[test]
reason = "Pre-filtered vector search (#548) is never attached to VERSION queries, and neither is the index: its entries and doc-ID mappings are not time-travel-aware, so a versioned KNN ranks the records read at the version as a brute-force search while the unversioned twin gets the bitmap prefilter."
issue = 548

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ category: 'a', id: pts:1, point: [10f] }, { category: 'b', id: pts:2, point: [20f] }, { category: 'a', id: pts:3, point: [30f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"VersionScope [ctx: Db] [version: $ts]
    SelectProject [ctx: Db] [projections: id]
        KnnTopK [ctx: Db] [field: point, k: 2, distance: Euclidean, dimension: deferred]
            TableScan [ctx: Db] [table: pts, direction: Forward, predicate: category = 'a', pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: category = 'a']
        KnnScan [ctx: Db] [index: hn_pt1, k: 2, ef: 40, dimension: 1]
            BitmapIndexScan [ctx: Db] [index: idx_category, access: = 'a']
"'''
*/

DEFINE FIELD category ON pts TYPE string;
DEFINE INDEX idx_category ON pts FIELDS category;
DEFINE INDEX hn_pt1 ON pts FIELDS point HNSW DIMENSION 1;
INSERT INTO pts [
	{ id: pts:1, point: [ 10f ], category: 'a' },
	{ id: pts:2, point: [ 20f ], category: 'b' },
	{ id: pts:3, point: [ 30f ], category: 'a' }
];
LET $pt = [0f];
LET $ts = time::now();
-- VERSION plans no index search: the nearest rows are ranked among the
-- records of a scan at the stamp, and no bitmap child appears.
EXPLAIN SELECT id FROM pts
	WHERE category = 'a' AND point <|2,40|> $pt VERSION $ts;
-- The unversioned twin gets the prefilter.
EXPLAIN SELECT id FROM pts
	WHERE category = 'a' AND point <|2,40|> $pt;
