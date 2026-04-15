/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Constant folding of time::now() in WHERE range bounds enables proper index range access and LIMIT pushdown"

# DEFINE INDEX
[[test.results]]
value = "None"

# Create 10 records with known dates in 2020
[[test.results]]
value = "None"

# ASC with time::now() as upper bound: all records are before 2026+, so all match
# With constant folding, time::now() becomes a literal datetime, enabling
# index range access and filter consumption for LIMIT pushdown.
[[test.results]]
value = "[{ id: test:1, ts: d'2020-01-02T00:00:00Z' }, { id: test:2, ts: d'2020-01-03T00:00:00Z' }, { id: test:3, ts: d'2020-01-04T00:00:00Z' }, { id: test:4, ts: d'2020-01-05T00:00:00Z' }, { id: test:5, ts: d'2020-01-06T00:00:00Z' }]"

# DESC with time::now() as upper bound
[[test.results]]
value = "[{ id: test:10, ts: d'2020-01-11T00:00:00Z' }, { id: test:9, ts: d'2020-01-10T00:00:00Z' }, { id: test:8, ts: d'2020-01-09T00:00:00Z' }, { id: test:7, ts: d'2020-01-08T00:00:00Z' }, { id: test:6, ts: d'2020-01-07T00:00:00Z' }]"

# Range with time::now() - duration arithmetic: same correctness
[[test.results]]
value = "[{ id: test:1, ts: d'2020-01-02T00:00:00Z' }, { id: test:2, ts: d'2020-01-03T00:00:00Z' }, { id: test:3, ts: d'2020-01-04T00:00:00Z' }, { id: test:4, ts: d'2020-01-05T00:00:00Z' }, { id: test:5, ts: d'2020-01-06T00:00:00Z' }]"

# time::now() - duration as lower bound (far future minus large duration = past)
# Since time::now() is ~2026 and we subtract 10y, the lower bound is ~2016,
# which is before all records (2020-01-02 to 2020-01-11), so all match.
[[test.results]]
value = "[{ id: test:1, ts: d'2020-01-02T00:00:00Z' }, { id: test:2, ts: d'2020-01-03T00:00:00Z' }, { id: test:3, ts: d'2020-01-04T00:00:00Z' }, { id: test:4, ts: d'2020-01-05T00:00:00Z' }, { id: test:5, ts: d'2020-01-06T00:00:00Z' }]"
*/

-- Index on timestamp field
DEFINE INDEX idx_ts ON test FIELDS ts;

-- Create 10 records with known dates (2020-01-02 through 2020-01-11)
{
    CREATE test:1  SET ts = d'2020-01-02T00:00:00Z';
    CREATE test:2  SET ts = d'2020-01-03T00:00:00Z';
    CREATE test:3  SET ts = d'2020-01-04T00:00:00Z';
    CREATE test:4  SET ts = d'2020-01-05T00:00:00Z';
    CREATE test:5  SET ts = d'2020-01-06T00:00:00Z';
    CREATE test:6  SET ts = d'2020-01-07T00:00:00Z';
    CREATE test:7  SET ts = d'2020-01-08T00:00:00Z';
    CREATE test:8  SET ts = d'2020-01-09T00:00:00Z';
    CREATE test:9  SET ts = d'2020-01-10T00:00:00Z';
    CREATE test:10 SET ts = d'2020-01-11T00:00:00Z';
    RETURN NONE;
};

-- ASC: time::now() as upper bound (all 2020 records < 2026+)
SELECT id, ts FROM test
  WHERE ts > d'2020-01-01T00:00:00Z' AND ts < time::now()
  ORDER BY ts ASC LIMIT 5;

-- DESC: same bounds, descending
SELECT id, ts FROM test
  WHERE ts > d'2020-01-01T00:00:00Z' AND ts < time::now()
  ORDER BY ts DESC LIMIT 5;

-- time::now() - 10y as lower bound, time::now() as upper bound
SELECT id, ts FROM test
  WHERE ts > time::now() - 10y AND ts < time::now()
  ORDER BY ts ASC LIMIT 5;

-- Both bounds are expressions: time::now() - 10y < ts < time::now() - 1d
SELECT id, ts FROM test
  WHERE ts > time::now() - 10y AND ts < time::now() - 1d
  ORDER BY ts ASC LIMIT 5;
