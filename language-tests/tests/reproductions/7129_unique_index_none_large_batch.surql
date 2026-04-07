/**
[env]
planner-strategy = ["all-ro"]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Unique index with >1000 NONE entries must not truncate results during prefix scan pagination"
issue = 7129

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
value = "1100"

[[test.results]]
value = "1100"

[[test.results]]
value = "1099"
*/

DEFINE TABLE OVERWRITE t SCHEMAFULL;
DEFINE FIELD OVERWRITE i ON t TYPE int;
DEFINE FIELD OVERWRITE j ON t TYPE option<string>;
DEFINE INDEX OVERWRITE t_idx ON t FIELDS i, j UNIQUE;

-- Create 1100 records where j is NONE (stored with non-unique key format).
-- This exceeds INDEX_BATCH_SIZE (1000) to exercise pagination.
FOR $n IN 0..1100 {
    CREATE type::record("t", $n) SET i = $n;
};

-- Baseline: how many records exist (no index involved)
RETURN count(SELECT * FROM t);

-- ORDER BY must return all records, not truncate at batch boundary
RETURN count(SELECT * FROM t ORDER BY i ASC);

-- Spot-check: the highest record must appear via DESC ORDER BY
RETURN (SELECT * FROM t ORDER BY i DESC LIMIT 1)[0].i;
