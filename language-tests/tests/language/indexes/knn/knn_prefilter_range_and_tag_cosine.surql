/**
[env]
timeout = 120000

[test]
reason = "Filtered KNN in the shape crud-bench sends — cosine, the predicate AND-ed after `<|k,ef|>`, no ORDER BY — with a one-sided range (`number_idx <= N`) and a string-tag equality (`status_idx = 'published'`) on typed, b-tree-indexed columns, so both reach the KNN pre-filter. Each ANN result is checked against crud-bench's brute-force reference (cosine similarity aliased and sorted DESC). On the exact tier (allow-list <= 2000 rows) the pre-filter scores the matching rows exactly, so the ANN ids must equal the brute-force ids; on the graph tier the walk is approximate, so it must return k matching rows, at least 7 of which are in the reference: the HNSW graph is built unseeded here, so a bound below k leaves room for a graph that differs from run to run. The schemaless twin columns (`number`, `status`) have no index and are tested per candidate; that path may return fewer than k rows, so it asserts that no non-matching row comes back, and, for the 33% tag, where about a third of the candidates match, that some rows do."

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
value = "[pts:1258, pts:1500, pts:1679, pts:1858, pts:2100, pts:2458, pts:2579, pts:2758, pts:3000, pts:3179]"

[[test.results]]
value = "[pts:1258, pts:1500, pts:1679, pts:1858, pts:2100, pts:2458, pts:2579, pts:2758, pts:3000, pts:3179]"

[[test.results]]
value = "[pts:1258, pts:1500, pts:1679, pts:1858, pts:2100, pts:2458, pts:2579, pts:2758, pts:3000, pts:3179]"

[[test.results]]
value = "[pts:2622, pts:2685, pts:2748, pts:2811, pts:2874, pts:2937, pts:3000, pts:3106, pts:3169, pts:3232]"

[[test.results]]
value = "[pts:2622, pts:2685, pts:2748, pts:2811, pts:2874, pts:2937, pts:3000, pts:3106, pts:3169, pts:3232]"

[[test.results]]
value = "[pts:235, pts:298, pts:361, pts:1366, pts:1429, pts:1492, pts:2434, pts:2497, pts:2560, pts:2623]"

[[test.results]]
value = "[pts:235, pts:298, pts:361, pts:1366, pts:1429, pts:1492, pts:2434, pts:2497, pts:2560, pts:2623]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ all_match: true, overlap_at_least_7: true, rows: 10 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ all_match: true, overlap_at_least_7: true, rows: 10 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ all_match: true, at_most_k: true }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ all_match: true, at_most_k: true, non_empty: true }"

[[test.results]]
value = "[dpts:1258, dpts:1500, dpts:1679, dpts:1858, dpts:2100, dpts:2458, dpts:2579, dpts:2758, dpts:3000, dpts:3179]"

[[test.results]]
value = "[dpts:1258, dpts:1500, dpts:1679, dpts:1858, dpts:2100, dpts:2458, dpts:2579, dpts:2758, dpts:3000, dpts:3179]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ all_match: true, at_most_k: true }"

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

-- Exact tier, one-sided range at 1%: ANN ids, brute force over the indexed
-- column, brute force over the unindexed twin (a full scan).
RETURN array::sort(SELECT VALUE id FROM pts WHERE embedding <|10,64|> $q AND number_idx <= 3);
RETURN array::sort((SELECT id, vector::similarity::cosine(embedding, $q) AS _d FROM pts WHERE number_idx <= 3 ORDER BY _d DESC LIMIT 10).id);
RETURN array::sort((SELECT id, vector::similarity::cosine(embedding, $q) AS _d FROM pts WHERE number <= 3 ORDER BY _d DESC LIMIT 10).id);

-- Exact tier, one-sided range at 10%.
RETURN array::sort(SELECT VALUE id FROM pts WHERE embedding <|10,64|> $q AND number_idx <= 30);
RETURN array::sort((SELECT id, vector::similarity::cosine(embedding, $q) AS _d FROM pts WHERE number_idx <= 30 ORDER BY _d DESC LIMIT 10).id);

-- Exact tier, string-tag equality.
RETURN array::sort(SELECT VALUE id FROM pts WHERE embedding <|10,64|> $q AND status_idx = 'published');
RETURN array::sort((SELECT id, vector::similarity::cosine(embedding, $q) AS _d FROM pts WHERE status_idx = 'published' ORDER BY _d DESC LIMIT 10).id);

-- Graph tier: k rows, every one matching, overlapping the exact reference.
LET $ann = SELECT id, number_idx FROM pts WHERE embedding <|10,64|> $q AND number_idx <= 200;
LET $ref = (SELECT id, vector::similarity::cosine(embedding, $q) AS _d FROM pts WHERE number_idx <= 200 ORDER BY _d DESC LIMIT 10).id;
RETURN { rows: array::len($ann), all_match: array::all($ann.number_idx, |$n| $n <= 200), overlap_at_least_7: array::len(array::intersect($ann.id, $ref)) >= 7 };
LET $ann = SELECT id, status_idx FROM pts WHERE embedding <|10,64|> $q AND (status_idx = 'published' OR status_idx = 'archived');
LET $ref = (SELECT id, vector::similarity::cosine(embedding, $q) AS _d FROM pts WHERE status_idx = 'published' OR status_idx = 'archived' ORDER BY _d DESC LIMIT 10).id;
RETURN { rows: array::len($ann), all_match: array::all($ann.status_idx, |$s| $s != 'draft'), overlap_at_least_7: array::len(array::intersect($ann.id, $ref)) >= 7 };

-- Unindexed schemaless columns: the predicate is tested per graph candidate.
LET $ann = SELECT id, number FROM pts WHERE embedding <|10,64|> $q AND number <= 3;
RETURN { at_most_k: array::len($ann) <= 10, all_match: array::all($ann.number, |$n| $n <= 3) };
LET $ann = SELECT id, status FROM pts WHERE embedding <|10,64|> $q AND status = 'published';
RETURN { non_empty: array::len($ann) > 0, at_most_k: array::len($ann) <= 10, all_match: array::all($ann.status, |$s| $s = 'published') };

-- DiskANN, exact tier, one-sided range at 1%.
RETURN array::sort(SELECT VALUE id FROM dpts WHERE embedding <|10,200|> $q AND number_idx <= 3);
RETURN array::sort((SELECT id, vector::similarity::cosine(embedding, $q) AS _d FROM dpts WHERE number_idx <= 3 ORDER BY _d DESC LIMIT 10).id);
LET $ann = SELECT id, number FROM dpts WHERE embedding <|10,200|> $q AND number <= 3;
RETURN { at_most_k: array::len($ann) <= 10, all_match: array::all($ann.number, |$n| $n <= 3) };
