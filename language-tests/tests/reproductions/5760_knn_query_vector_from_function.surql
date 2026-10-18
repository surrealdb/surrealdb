/**
[test]
reason = "A kNN operand that is a function call is evaluated once and its distance is reported by vector::distance::knn()"
issue = 5760

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ e: [1f, 0f, 0f], id: p:1 }]"

[[test.results]]
value = "[{ e: [0f, 1f, 0f], id: p:2 }]"

[[test.results]]
value = "[{ e: [0.5f, 0.5f, 0f], id: p:3 }]"

[[test.results]]
value = "[{ d: 0f, id: p:1 }, { d: 0.29289321881345254f, id: p:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ d: 0f, id: p:1 }, { d: 0.29289321881345254f, id: p:3 }]"
*/

DEFINE FUNCTION fn::q() { RETURN [1.0, 0.0, 0.0]; };
CREATE p:1 SET e = [1.0, 0.0, 0.0];
CREATE p:2 SET e = [0.0, 1.0, 0.0];
CREATE p:3 SET e = [0.5, 0.5, 0.0];
SELECT id, vector::distance::knn() AS d FROM p WHERE e <|2,COSINE|> fn::q() ORDER BY d;
DEFINE INDEX ix ON p FIELDS e HNSW DIMENSION 3 DIST COSINE;
SELECT id, vector::distance::knn() AS d FROM p WHERE e <|2,40|> fn::q() ORDER BY d;
