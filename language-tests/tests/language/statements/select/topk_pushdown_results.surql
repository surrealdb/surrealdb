/**
[env]
namespace = true
database = true

[test]
reason = "TopK threshold pushdown result correctness on unindexed scans: ORDER BY + LIMIT results must equal the prefix of the full ordered result (LIMIT above the priority-queue cap forces SortByKey, the pushdown-free reference), including missing sort fields (None sorts below everything), mixed-type sort keys, multi-key ORDER BY with heavy first-key ties (equal first keys must fall through to the later keys), and WHERE + ORDER BY + LIMIT with the pre-decode WHERE probe active in the same scan visitor."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:39, t:38, t:37, t:36, t:35]"

[[test.results]]
value = "[t:39, t:38, t:37, t:36, t:35]"

[[test.results]]
value = "[t:0, t:1, t:2, t:3, t:4]"

[[test.results]]
value = "[t:25, t:24, t:23, t:5]"

[[test.results]]
value = "[t:25, t:24, t:23, t:5]"

[[test.results]]
value = "[{ id: t:100, n: 99 }]"

[[test.results]]
value = "[{ id: t:101, n: 98 }]"

[[test.results]]
value = "[t:100, t:101, t:0]"

[[test.results]]
value = "[t:0, t:100, t:101]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: mixed:1, k: 5 }]"

[[test.results]]
value = "[{ id: mixed:2, k: 'abc' }]"

[[test.results]]
value = "[{ id: mixed:3, k: d'2024-06-01T00:00:00Z' }]"

[[test.results]]
value = "[{ id: mixed:4, k: 1 }]"

[[test.results]]
value = "[{ id: mixed:5, k: 'zzz' }]"

[[test.results]]
value = "[mixed:3, mixed:5, mixed:2]"

[[test.results]]
value = "[mixed:3, mixed:5, mixed:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[2, 2], [2, 5], [2, 8], [2, 11], [2, 14]]"

[[test.results]]
value = "[[2, 2], [2, 5], [2, 8], [2, 11], [2, 14]]"
*/

DEFINE TABLE t SCHEMALESS;
FOR $i IN 0..40 {
	CREATE type::record('t', $i) SET n = ($i * 7) % 20, dt = d'2024-01-01T00:00:00Z' + duration::from_mins($i);
};

-- Heap path (LIMIT <= 1000, probe armed) vs full-sort reference (LIMIT 1001
-- forces SortByKey — no heap, no pushdown): prefixes must agree.
SELECT VALUE id FROM t WITH NOINDEX ORDER BY dt DESC LIMIT 5;
SELECT VALUE id FROM (SELECT id, dt FROM t WITH NOINDEX ORDER BY dt DESC LIMIT 1001) LIMIT 5;
SELECT VALUE id FROM t WITH NOINDEX ORDER BY dt ASC LIMIT 5;

-- WHERE + ORDER BY + LIMIT: the IN-set pre-decode probe and the threshold
-- probe run in the same visitor.
SELECT VALUE id FROM t WITH NOINDEX WHERE n IN [1, 8, 15] ORDER BY dt DESC LIMIT 4;
SELECT VALUE id FROM (SELECT id, dt FROM t WITH NOINDEX WHERE n IN [1, 8, 15] ORDER BY dt DESC LIMIT 1001) LIMIT 4;

-- Rows missing the sort field read as None: last under DESC, first under ASC.
CREATE t:100 SET n = 99;
CREATE t:101 SET n = 98;
SELECT VALUE id FROM t WITH NOINDEX ORDER BY dt ASC LIMIT 3;
SELECT VALUE id FROM t WITH NOINDEX ORDER BY dt DESC START 39 LIMIT 3;

-- Mixed-type sort keys follow Value's total type ordering, identically with
-- and without the heap.
DEFINE TABLE mixed SCHEMALESS;
CREATE mixed:1 SET k = 5;
CREATE mixed:2 SET k = 'abc';
CREATE mixed:3 SET k = d'2024-06-01T00:00:00Z';
CREATE mixed:4 SET k = 1;
CREATE mixed:5 SET k = 'zzz';
SELECT VALUE id FROM mixed WITH NOINDEX ORDER BY k DESC LIMIT 3;
SELECT VALUE id FROM (SELECT id, k FROM mixed WITH NOINDEX ORDER BY k DESC LIMIT 1001) LIMIT 3;

-- Multi-key ORDER BY with heavy ties on the first key: an equal first key
-- cannot be rejected by the probe — the second key decides.
DEFINE TABLE m SCHEMALESS;
FOR $i IN 0..30 {
	CREATE type::record('m', $i) SET a = $i % 3, b = $i;
};
SELECT VALUE [a, b] FROM m WITH NOINDEX ORDER BY a DESC, b ASC LIMIT 5;
SELECT VALUE [a, b] FROM (SELECT a, b FROM m WITH NOINDEX ORDER BY a DESC, b ASC LIMIT 1001) LIMIT 5;
