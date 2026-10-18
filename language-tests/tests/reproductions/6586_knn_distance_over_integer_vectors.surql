/**
[test]
reason = "vector::distance::knn() over an HNSW index of integer-typed vectors reports the real distance, not 0"
issue = 6586

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ d: 0f, id: a:0 }, { d: 1.7320508075688772f, id: a:1 }, { d: 3.4641016151377544f, id: a:2 }]"

[[test.results]]
value = "[{ d: 0f, id: b:0 }, { d: 1.7320508075688772f, id: b:1 }, { d: 3.4641016151377544f, id: b:2 }]"
*/

DEFINE INDEX i64 ON TABLE a FIELDS e HNSW DIMENSION 3 TYPE I64;
DEFINE INDEX i16 ON TABLE b FIELDS e HNSW DIMENSION 3 TYPE I16;
FOR $i IN 0..6 { CREATE type::record('a', $i) SET e = [$i, $i, $i]; CREATE type::record('b', $i) SET e = [$i, $i, $i]; };
SELECT id, vector::distance::knn() AS d FROM a WHERE e <|3,40|> [0,0,0] ORDER BY d;
SELECT id, vector::distance::knn() AS d FROM b WHERE e <|3,40|> [0,0,0] ORDER BY d;
