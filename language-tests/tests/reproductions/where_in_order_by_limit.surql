/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "WHERE IN + ORDER BY + LIMIT must return correct row count when a separate ORDER BY index exists"

# DEFINE INDEX idx_status
[[test.results]]
value = "None"

# DEFINE INDEX idx_ts (separate ORDER BY index — key to triggering the bug)
[[test.results]]
value = "None"

# FOR loop creating 30 records
[[test.results]]
value = "None"

# ASC LIMIT 8: first 8 of 20 matching rows by ts order
[[test.results]]
value = "[{ id: item:1, status: 'active', ts: d'2025-01-01T00:00:00Z' }, { id: item:2, status: 'pending', ts: d'2025-01-02T00:00:00Z' }, { id: item:4, status: 'active', ts: d'2025-01-04T00:00:00Z' }, { id: item:5, status: 'pending', ts: d'2025-01-05T00:00:00Z' }, { id: item:7, status: 'active', ts: d'2025-01-07T00:00:00Z' }, { id: item:8, status: 'pending', ts: d'2025-01-08T00:00:00Z' }, { id: item:10, status: 'active', ts: d'2025-01-10T00:00:00Z' }, { id: item:11, status: 'pending', ts: d'2025-01-11T00:00:00Z' }]"

# DESC LIMIT 8: last 8 of 20 matching rows by ts order
[[test.results]]
value = "[{ id: item:29, status: 'pending', ts: d'2025-01-29T00:00:00Z' }, { id: item:28, status: 'active', ts: d'2025-01-28T00:00:00Z' }, { id: item:26, status: 'pending', ts: d'2025-01-26T00:00:00Z' }, { id: item:25, status: 'active', ts: d'2025-01-25T00:00:00Z' }, { id: item:23, status: 'pending', ts: d'2025-01-23T00:00:00Z' }, { id: item:22, status: 'active', ts: d'2025-01-22T00:00:00Z' }, { id: item:20, status: 'pending', ts: d'2025-01-20T00:00:00Z' }, { id: item:19, status: 'active', ts: d'2025-01-19T00:00:00Z' }]"
*/

-- Two separate indexes: idx_status for WHERE, idx_ts for ORDER BY.
-- The planner may choose idx_ts for ORDER BY coverage and incorrectly push LIMIT
-- into the IndexScan, causing the WHERE filter to reduce results below LIMIT.
DEFINE INDEX idx_status ON item FIELDS status;
DEFINE INDEX idx_ts ON item FIELDS ts;

-- Create 30 records: cycling active/pending/archived (10 each)
FOR $i IN 1..=30 {
    CREATE type::record('item', $i) SET
        status = ['active', 'pending', 'archived'][($i - 1) % 3],
        ts = d'2025-01-01T00:00:00Z' + duration::from_days($i - 1);
};

-- 20 matching rows (active + pending), LIMIT 8 — must return exactly 8
SELECT id, status, ts FROM item
  WHERE status IN ['active', 'pending']
  ORDER BY ts ASC LIMIT 8;

SELECT id, status, ts FROM item
  WHERE status IN ['active', 'pending']
  ORDER BY ts DESC LIMIT 8;
