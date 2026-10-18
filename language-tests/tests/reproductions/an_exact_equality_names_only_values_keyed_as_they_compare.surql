/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An index that has recorded one entry per record answers an equality exactly,
so its bitmap stands in for the conjunct: a KNN allow-list drops the residual
and a NOT subtracts the bitmap. That holds only for a value whose key is the
key of everything it equals. An object or a record id with a compound key may
hold a number a float and a decimal do not both hold, such as `0.1`, and a
regex equals every string it matches, each stored under its own key, so such
an equality is judged on the rows and answers as `WITH NOINDEX` does.
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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[ks:2]"

[[test.results]]
value = "[ks:2]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[ks:4]"

[[test.results]]
value = "[ks:4]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[ks:5]"

[[test.results]]
value = "[ks:5]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"


*/

DEFINE INDEX idx_c ON ks FIELDS c;
DEFINE INDEX idx_d ON ks FIELDS d;
DEFINE INDEX idx_vec ON ks FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN;
CREATE ks:1 SET c = foo:[0.1f], d = 1, vec = [0.1, 0.1] RETURN NONE;
CREATE ks:2 SET c = foo:[0.1dec], d = 1, vec = [0.2, 0.2] RETURN NONE;
CREATE ks:3 SET c = { a: 0.1f }, d = 1, vec = [0.3, 0.3] RETURN NONE;
CREATE ks:4 SET c = { a: 0.1dec }, d = 1, vec = [0.4, 0.4] RETURN NONE;
CREATE ks:5 SET c = 'zz', d = 1, vec = [0.5, 0.5] RETURN NONE;
CREATE ks:6 SET c = 5, d = 2, vec = [0.6, 0.6] RETURN NONE;
SELECT VALUE id FROM ks WHERE c = foo:[0.1dec] AND vec <|6,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM ks WITH NOINDEX WHERE c = foo:[0.1dec] AND vec <|6,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
SELECT count() FROM ks WHERE c = foo:[0.1dec] AND d = 1 GROUP ALL;
SELECT count() FROM ks WITH NOINDEX WHERE c = foo:[0.1dec] AND d = 1 GROUP ALL;
SELECT count() FROM ks WHERE d = 1 AND NOT (c = foo:[0.1dec]) GROUP ALL;
SELECT count() FROM ks WITH NOINDEX WHERE d = 1 AND NOT (c = foo:[0.1dec]) GROUP ALL;
SELECT VALUE id FROM ks WHERE c = { a: 0.1dec } AND vec <|6,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM ks WITH NOINDEX WHERE c = { a: 0.1dec } AND vec <|6,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
SELECT count() FROM ks WHERE d = 1 AND NOT (c = { a: 0.1dec }) GROUP ALL;
SELECT count() FROM ks WITH NOINDEX WHERE d = 1 AND NOT (c = { a: 0.1dec }) GROUP ALL;
SELECT VALUE id FROM ks WHERE c = /zz/ AND vec <|6,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM ks WITH NOINDEX WHERE c = /zz/ AND vec <|6,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
SELECT count() FROM ks WHERE d = 1 AND NOT (c = /zz/) GROUP ALL;
SELECT count() FROM ks WITH NOINDEX WHERE d = 1 AND NOT (c = /zz/) GROUP ALL;
