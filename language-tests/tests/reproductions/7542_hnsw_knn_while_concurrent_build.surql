/**
[env]
timeout = 30000

[test]
reason = "A kNN query over a vector index whose CONCURRENTLY build has not finished ranks the rows by brute force: it answers k rows, never none"
issue = 7542

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
value = "[10, 10, true]"
*/
FOR $i IN 0..800 { CREATE type::record('t', $i) SET v = [$i, $i % 7, $i % 13] RETURN NONE; };
DEFINE INDEX ix ON t FIELDS v HNSW DIMENSION 3 DIST EUCLIDEAN TYPE F32 CONCURRENTLY;
LET $r1 = SELECT VALUE id FROM t WHERE v <|10,100|> [250, 3, 6];
SLEEP 2s;
LET $r2 = SELECT VALUE id FROM t WHERE v <|10,100|> [250, 3, 6];
LET $exact = SELECT VALUE id FROM t WHERE v <|10,EUCLIDEAN|> [250, 3, 6];
RETURN [array::len($r1), array::len($r2), array::sort($r1) = array::sort($exact)];
