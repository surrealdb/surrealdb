/**
[env]
timeout = 120000

[test]
reason = "The plans behind knn_prefilter_range_and_tag_cosine.surql, on the same rows: a one-sided range and a string-tag equality on typed, b-tree-indexed columns become a BitmapIndexScan allow-list under the KnnScan, on the exact tier at 1% / 10% / 'published' (<= 2000 rows) and the graph tier at 67%; the same predicates on the schemaless twin columns stay a per-candidate KnnScan predicate. Without this, a pre-filter that silently stopped engaging would leave the companion comparing two slower paths."

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
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id] {rows: 10}
    Filter [ctx: Db] [predicate: number_idx <= 3] {rows: 10}
        KnnScan [ctx: Db] [index: pts_hnsw, k: 10, ef: 64, dimension: 3, prefilter_tier: exact] {rows: 10}
            BitmapIndexScan [ctx: Db] [index: pts_number_idx, access: <=3] {rows: 33}

Total rows: 10''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id] {rows: 10}
    Filter [ctx: Db] [predicate: number_idx <= 30] {rows: 10}
        KnnScan [ctx: Db] [index: pts_hnsw, k: 10, ef: 64, dimension: 3, prefilter_tier: exact] {rows: 10}
            BitmapIndexScan [ctx: Db] [index: pts_number_idx, access: <=30] {rows: 330}

Total rows: 10''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id] {rows: 10}
    Filter [ctx: Db] [predicate: status_idx = 'published'] {rows: 10}
        KnnScan [ctx: Db] [index: pts_hnsw, k: 10, ef: 64, dimension: 3, prefilter_tier: exact] {rows: 10}
            BitmapIndexScan [ctx: Db] [index: pts_status_idx, access: = 'published'] {rows: 1100}

Total rows: 10"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id] {rows: 10}
    Filter [ctx: Db] [predicate: number_idx <= 200] {rows: 10}
        KnnScan [ctx: Db] [index: pts_hnsw, k: 10, ef: 64, dimension: 3, prefilter_tier: graph, prefilter_ef: 64] {rows: 10}
            BitmapIndexScan [ctx: Db] [index: pts_number_idx, access: <=200] {rows: 2200}

Total rows: 10''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id] {rows: 10}
    Filter [ctx: Db] [predicate: status_idx = 'published' OR status_idx = 'archived'] {rows: 10}
        KnnScan [ctx: Db] [index: pts_hnsw, k: 10, ef: 64, dimension: 3, prefilter_tier: graph, prefilter_ef: 64] {rows: 10}
            BitmapOr [ctx: Db] {rows: 2200}
                BitmapIndexScan [ctx: Db] [index: pts_status_idx, access: = 'published'] {rows: 1100}
                BitmapIndexScan [ctx: Db] [index: pts_status_idx, access: = 'archived'] {rows: 1100}

Total rows: 10"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: number <= 3]
        KnnScan [ctx: Db] [index: pts_hnsw, k: 10, ef: 64, dimension: 3, predicate: number <= 3]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: status = 'published']
        KnnScan [ctx: Db] [index: pts_hnsw, k: 10, ef: 64, dimension: 3, predicate: status = 'published']
"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id] {rows: 10}
    Filter [ctx: Db] [predicate: number_idx <= 3] {rows: 10}
        KnnScan [ctx: Db] [index: dpts_diskann, k: 10, ef: 200, dimension: 3, prefilter_tier: exact] {rows: 10}
            BitmapIndexScan [ctx: Db] [index: dpts_number_idx, access: <=3] {rows: 33}

Total rows: 10''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: number <= 3]
        KnnScan [ctx: Db] [index: dpts_diskann, k: 10, ef: 200, dimension: 3, predicate: number <= 3]
''''

*/

DEFINE TABLE pts SCHEMALESS;
DEFINE FIELD number_idx ON pts TYPE int;
DEFINE FIELD status_idx ON pts TYPE string;
DEFINE INDEX pts_number_idx ON pts FIELDS number_idx;
DEFINE INDEX pts_status_idx ON pts FIELDS status_idx;
DEFINE TABLE dpts SCHEMALESS;
DEFINE FIELD number_idx ON dpts TYPE int;
DEFINE INDEX dpts_number_idx ON dpts FIELDS number_idx;
-- 3300 rows. `number` is ((i * 7919) % 300) + 1, so every value in 1..300
-- occurs exactly 11 times: `<= 3` is 33 rows (1%), `<= 30` is 330 (10%), both
-- on the exact tier, and `<= 200` is 2200 (67%), on the graph tier. `status`
-- cycles with i % 3: 'published' is 1100 rows (exact tier) and
-- 'published' OR 'archived' 2200 (graph tier). Vector components are
-- irrational, so no two rows tie on cosine distance and the brute-force order
-- is unique.
FOR $i IN 0..3300 {
	LET $row = {
		embedding: [math::sin($i), math::cos($i * 1.3), math::sin($i * 0.7 + 1)],
		number: (($i * 7919) % 300) + 1,
		status: ['draft', 'published', 'archived'][$i % 3],
	};
	LET $row = $row + { number_idx: $row.number, status_idx: $row.status };
	CREATE type::record('pts', $i) CONTENT $row RETURN NONE;
	CREATE type::record('dpts', $i) CONTENT $row RETURN NONE;
};
-- The vector indexes are defined after the rows, as their own statements, so
-- the blocking build puts every vector in the graph. Defined before the rows,
-- the vectors would wait in the compaction backlog (the test runner runs no
-- compaction), and a query would score that backlog exhaustively instead of
-- walking a graph. `compacting: false` confirms no backlog is left.
DEFINE INDEX pts_hnsw ON pts FIELDS embedding HNSW DIMENSION 3 DIST COSINE EFC 200 M 16;
DEFINE INDEX dpts_diskann ON dpts FIELDS embedding DISKANN DIMENSION 3 DIST COSINE DEGREE 64 L_BUILD 100 ALPHA 1.2;
RETURN (INFO FOR INDEX pts_hnsw ON pts).building.compacting;
RETURN (INFO FOR INDEX dpts_diskann ON dpts).building.compacting;
LET $q = [0.3f, -0.5f, 0.8f];

EXPLAIN ANALYZE SELECT id FROM pts WHERE embedding <|10,64|> $q AND number_idx <= 3;
EXPLAIN ANALYZE SELECT id FROM pts WHERE embedding <|10,64|> $q AND number_idx <= 30;
EXPLAIN ANALYZE SELECT id FROM pts WHERE embedding <|10,64|> $q AND status_idx = 'published';
EXPLAIN ANALYZE SELECT id FROM pts WHERE embedding <|10,64|> $q AND number_idx <= 200;
EXPLAIN ANALYZE SELECT id FROM pts WHERE embedding <|10,64|> $q AND (status_idx = 'published' OR status_idx = 'archived');
EXPLAIN SELECT id FROM pts WHERE embedding <|10,64|> $q AND number <= 3;
EXPLAIN SELECT id FROM pts WHERE embedding <|10,64|> $q AND status = 'published';
EXPLAIN ANALYZE SELECT id FROM dpts WHERE embedding <|10,200|> $q AND number_idx <= 3;
EXPLAIN SELECT id FROM dpts WHERE embedding <|10,200|> $q AND number <= 3;
