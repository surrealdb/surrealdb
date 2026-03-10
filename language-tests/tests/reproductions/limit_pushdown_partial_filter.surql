/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "LIMIT must not be pushed to IndexScan when a non-indexed field in the WHERE clause creates a residual filter"

# DEFINE INDEX
[[test.results]]
value = "None"

# FOR loop creating 20 records
[[test.results]]
value = "None"

# ASC: first 5 active records in score order
[[test.results]]
value = "[{ id: test:4, score: 40 }, { id: test:6, score: 60 }, { id: test:8, score: 80 }, { id: test:10, score: 100 }, { id: test:12, score: 120 }]"

# DESC: last 5 active records in score order
[[test.results]]
value = "[{ id: test:16, score: 160 }, { id: test:14, score: 140 }, { id: test:12, score: 120 }, { id: test:10, score: 100 }, { id: test:8, score: 80 }]"

*/

-- Index on score only; the active field is NOT indexed,
-- so WHERE active = true becomes a residual filter.
DEFINE INDEX idx_score ON test FIELDS score;

FOR $i IN 1..=20 {
    CREATE type::record('test', $i) SET
        score = $i * 10,
        active = ($i % 2 == 0);
};

-- Range score > 20 AND score < 180 yields 15 rows (scores 30..170).
-- Of those, 7 are active (scores 40, 60, 80, 100, 120, 140, 160).
-- LIMIT 5 must return exactly 5 after the residual filter, not be
-- pushed into the IndexScan which would only emit 5 pre-filter rows.
SELECT id, score FROM test
  WHERE score > 20 AND score < 180 AND active = true
  ORDER BY score ASC LIMIT 5;

SELECT id, score FROM test
  WHERE score > 20 AND score < 180 AND active = true
  ORDER BY score DESC LIMIT 5;
