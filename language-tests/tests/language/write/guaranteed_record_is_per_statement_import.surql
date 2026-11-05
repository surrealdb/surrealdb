/**
[test]
reason = "Setup: a table larger than one scan batch, an empty table, and a small table — the three inputs that tell a per-statement guaranteed record apart from a per-batch or per-record one."
run = false
*/

DEFINE TABLE many SCHEMALESS;
DEFINE TABLE empty SCHEMALESS;
DEFINE TABLE few SCHEMALESS;

-- More rows than NORMAL_BATCH_SIZE, so a fallback that fired once per batch
-- would be visible as more than one created record.
FOR $i IN 0..600 {
	CREATE type::record('many', $i) SET n = 1;
};

CREATE few:1 SET n = 1;
CREATE few:2 SET n = 2;
CREATE few:3 SET n = 3;
