/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "LIMIT must not be pushed to IndexScan when there is a residual filter (expression-valued WHERE bounds)"

# DEFINE INDEX
[[test.results]]
value = "None"

# FOR loop creating 10 records
[[test.results]]
value = "None"

# ASC with literal bounds: 5 rows (records 3–7)
[[test.results]]
value = "[{ id: test:3, score: 30 }, { id: test:4, score: 40 }, { id: test:5, score: 50 }, { id: test:6, score: 60 }, { id: test:7, score: 70 }]"

# DESC with literal bounds: 5 rows (records 7–3)
[[test.results]]
value = "[{ id: test:7, score: 70 }, { id: test:6, score: 60 }, { id: test:5, score: 50 }, { id: test:4, score: 40 }, { id: test:3, score: 30 }]"

# ASC with expression bounds: must also return 5 rows
[[test.results]]
value = "[{ id: test:3, score: 30 }, { id: test:4, score: 40 }, { id: test:5, score: 50 }, { id: test:6, score: 60 }, { id: test:7, score: 70 }]"

# DESC with expression bounds: must also return 5 rows
[[test.results]]
value = "[{ id: test:7, score: 70 }, { id: test:6, score: 60 }, { id: test:5, score: 50 }, { id: test:4, score: 40 }, { id: test:3, score: 30 }]"
*/

-- Index on numeric score field (not datetime — proves bug is type-agnostic)
DEFINE INDEX idx_score ON test FIELDS score;

-- Create 10 records with known scores
FOR $i IN 1..=10 {
    CREATE type::record('test', $i) SET score = $i * 10;
};

-- Literal bounds: range 20 < score < 80 → 5 matching rows (30,40,50,60,70)
-- LIMIT can be pushed to IndexScan since filter is fully consumed.
SELECT id, score FROM test
  WHERE score > 20 AND score < 80
  ORDER BY score ASC LIMIT 5;

SELECT id, score FROM test
  WHERE score > 20 AND score < 80
  ORDER BY score DESC LIMIT 5;

-- Expression bounds: same logical range but using math::floor/ceil.
-- LIMIT must NOT be pushed — residual Filter needed above the scan.
SELECT id, score FROM test
  WHERE score > math::floor(20.5) AND score < math::ceil(79.5)
  ORDER BY score ASC LIMIT 5;

SELECT id, score FROM test
  WHERE score > math::floor(20.5) AND score < math::ceil(79.5)
  ORDER BY score DESC LIMIT 5;
