/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A scalar-only index answers an equality exactly, so its bitmap stands in for
the conjunct: a KNN allow-list drops the residual. A float and a decimal below one that print alike compare
equal yet are keyed by their exact values, so `c = 0.1dec` probes a key that
holds neither `0.1f` nor the other way round. An equality that names such a
number is judged on the rows, whatever the index has recorded, and the
filtered search answers as the scan does.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[ks:1, ks:2]"

[[test.results]]
value = "[ks:1, ks:2]"

[[test.results]]
value = "[ks:1, ks:2]"

[[test.results]]
value = "[ks:1, ks:2]"
*/
DEFINE INDEX idx_c ON ks FIELDS c;
DEFINE INDEX idx_d ON ks FIELDS d;
DEFINE INDEX idx_vec ON ks FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN;
CREATE ks:1 SET c = 0.1f, d = 1, vec = [0.1, 0.1] RETURN NONE;
CREATE ks:2 SET c = 0.1dec, d = 1, vec = [0.2, 0.2] RETURN NONE;
CREATE ks:3 SET c = 5, d = 2, vec = [0.3, 0.3] RETURN NONE;
SELECT VALUE id FROM ks WHERE c = 0.1dec AND vec <|3,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM ks WITH NOINDEX WHERE c = 0.1dec AND vec <|3,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM ks WHERE c = 0.1f AND vec <|3,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM ks WITH NOINDEX WHERE c = 0.1f AND vec <|3,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
