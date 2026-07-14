/**
[test]
reason = "Differential check: bitmap fusion plans (all-ro) return identical results to the legacy engine (compute-only) for AND-composed selective predicates, including AND NOT, nested OR and ranges. ORDER BY uses an unindexed field so the bitmap plan actually drives the all-ro runs (a covered ORDER BY keeps the streaming plan)."
issue = 547

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
value = "[{ id: items:0, pos: 0 }, { id: items:6, pos: 6 }, { id: items:12, pos: 12 }, { id: items:18, pos: 18 }, { id: items:24, pos: 24 }, { id: items:30, pos: 30 }, { id: items:36, pos: 36 }, { id: items:42, pos: 42 }, { id: items:48, pos: 48 }, { id: items:54, pos: 54 }]"

[[test.results]]
value = "[{ id: items:31, pos: 31 }, { id: items:34, pos: 34 }, { id: items:37, pos: 37 }, { id: items:40, pos: 40 }, { id: items:43, pos: 43 }, { id: items:46, pos: 46 }, { id: items:49, pos: 49 }]"

[[test.results]]
value = "[{ id: items:5, pos: 5 }, { id: items:11, pos: 11 }, { id: items:17, pos: 17 }, { id: items:23, pos: 23 }, { id: items:29, pos: 29 }, { id: items:35, pos: 35 }, { id: items:41, pos: 41 }, { id: items:47, pos: 47 }, { id: items:53, pos: 53 }, { id: items:59, pos: 59 }]"

[[test.results]]
value = "[{ id: items:1, pos: 1 }, { id: items:2, pos: 2 }, { id: items:4, pos: 4 }, { id: items:5, pos: 5 }, { id: items:7, pos: 7 }, { id: items:8, pos: 8 }]"

[[test.results]]
value = "[{ id: items:0, pos: 0 }, { id: items:2, pos: 2 }, { id: items:6, pos: 6 }, { id: items:8, pos: 8 }, { id: items:12, pos: 12 }, { id: items:14, pos: 14 }, { id: items:18, pos: 18 }]"

[[test.results]]
value = "[{ id: items:43, pos: 43 }, { id: items:45, pos: 45 }, { id: items:49, pos: 49 }, { id: items:51, pos: 51 }, { id: items:55, pos: 55 }, { id: items:57, pos: 57 }]"

[[test.results]]
value = "[]"
*/

DEFINE FIELD category ON items TYPE string;
DEFINE FIELD flag ON items TYPE bool;
DEFINE FIELD score ON items TYPE int;
DEFINE INDEX idx_category ON items FIELDS category;
DEFINE INDEX idx_flag ON items FIELDS flag;
DEFINE INDEX idx_score ON items FIELDS score;

-- 60 records over 3 categories, alternating flag, score 0..59.
-- `pos` is deliberately unindexed: ordering by it forces a Sort on every
-- engine, so plan choice cannot change row order.
FOR $i IN 0..60 {
    CREATE type::record('items', $i) SET
        category = ['red', 'green', 'blue'][$i % 3],
        flag = $i % 2 == 0,
        score = $i,
        pos = $i;
};

-- AND of two selective equality predicates.
SELECT id, pos FROM items WHERE category = 'red' AND flag = true ORDER BY pos;

-- AND of equality and range predicates.
SELECT id, pos FROM items WHERE category = 'green' AND score > 30 AND score <= 50 ORDER BY pos;

-- Conjunctive NOT.
SELECT id, pos FROM items WHERE category = 'blue' AND NOT (flag = true) ORDER BY pos;
SELECT id, pos FROM items WHERE score < 10 AND !(category = 'red') ORDER BY pos;

-- OR nested in AND.
SELECT id, pos FROM items WHERE flag = true AND (category = 'red' OR category = 'blue') AND score < 20 ORDER BY pos;

-- IN nested in AND.
SELECT id, pos FROM items WHERE flag = false AND category IN ['red', 'green'] AND score >= 40 ORDER BY pos;

-- Empty intersection.
SELECT id FROM items WHERE category = 'red' AND category = 'green';

-- NOTE: index-backed `count()` differential checks live in
-- bitmap_fusion_count_new_executor.surql (with NOINDEX baselines): the
-- legacy compute engine has a pre-existing bug where a multi-index AND
-- count reports a single branch's cardinality.
