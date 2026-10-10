/**
[test]
reason = "A kNN query over an HNSW index answers with k distinct rows straight after a bulk load, while the load is still queued for compaction"
issue = 7543

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "10"

[[test.results]]
value = "10"

[[test.results]]
value = "true"
*/

DEFINE INDEX ix ON t FIELDS v HNSW DIMENSION 3 DIST EUCLIDEAN TYPE F32;
FOR $i IN 0..500 { CREATE type::record('t', $i) SET v = [$i, $i % 7, $i % 13] RETURN NONE; };
LET $ids = SELECT VALUE id FROM t WHERE v <|10,100|> [250, 3, 6];
RETURN array::len($ids);
RETURN array::len(array::distinct($ids));
RETURN type::is_bool((INFO FOR INDEX ix ON t).building.compacting);
