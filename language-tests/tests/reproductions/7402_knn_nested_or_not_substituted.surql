/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro"]

[test]
reason = "Correctness guard for the nested-OR-in-AND union substitution (issue #7402): a KNN (nearest-neighbour) operator must keep driving from its KnnScan and must NOT be replaced by a higher-scoring OR-of-equalities union. `field <|k|> $v AND <filter>` is filtered-KNN — the k nearest rows that satisfy the filter. If the plan drove from the email union instead, the KNN operator would become a residual filter that no longer limits to k, so ALL filter-matching rows (including far ones) would be returned. With k=2 over three unique-email branches the correct answer is the two nearest matches [pts:1, pts:2]; the buggy union plan would also return the far pts:3."
issue = 7402

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ email: 'a', id: pts:1, vec: [0f, 0f] }]"

[[test.results]]
value = "[{ email: 'b', id: pts:2, vec: [1f, 1f] }]"

[[test.results]]
value = "[{ email: 'c', id: pts:3, vec: [100f, 100f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }]"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id] {rows: 2}\n    Filter [ctx: Db] [predicate: email = 'a' OR email = 'b' OR email = 'c'] {rows: 2}\n        KnnScan [ctx: Db] [index: pt_hnsw, k: 2, ef: 100, dimension: 2, predicate: email = 'a' OR email = 'b' OR email = 'c'] {rows: 2}\n\nTotal rows: 2"'''

*/

DEFINE INDEX pt_hnsw ON pts FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN;
DEFINE INDEX pt_email ON pts FIELDS email UNIQUE;

CREATE pts:1 SET vec = [0.0, 0.0], email = 'a';
CREATE pts:2 SET vec = [1.0, 1.0], email = 'b';
CREATE pts:3 SET vec = [100.0, 100.0], email = 'c';

LET $pt = [0.0, 0.0];

-- KNN alone (k=2): the two nearest to [0,0] are pts:1 and pts:2.
SELECT id FROM pts WHERE vec <|2,100|> $pt;

-- KNN AND a nested OR of three unique-email equalities. The union (score 1000) outscores the
-- KNN (800), but the substitution must be skipped. Filtered-KNN semantics: the 2 nearest rows
-- matching email in {a,b,c} = [pts:1, pts:2]. pts:3 (far) must NOT appear — a union plan would
-- return all three matches and drop the k limit.
SELECT id FROM pts WHERE vec <|2,100|> $pt AND (email = 'a' OR email = 'b' OR email = 'c');

-- The plan must drive from the KNN scan (KNN not substituted by the email union).
EXPLAIN ANALYZE SELECT id FROM pts WHERE vec <|2,100|> $pt AND (email = 'a' OR email = 'b' OR email = 'c');
