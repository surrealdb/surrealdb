/**
[env]
# The scope carried here is the streaming planner's; the legacy compute
# executor has no query planner for the subquery and evaluates both index
# functions to NONE, so pinning one set of results means pinning this mode.
planner-strategy = ["all-ro"]

[test]
reason = "A nested SELECT that declares no MATCHES / KNN of its own inherits the enclosing planner's scope, so an index function inside it resolves against the operator that introduced it instead of failing to plan with 'no MATCHES clause found in WHERE condition' / 'no KNN operator found in WHERE condition'. Pins the inherit-then-override rule in Planner::set_planning_scopes: the inner SELECT reads the outer statement's MATCHES entries and KNN distance slot, so blog:2 scores through the outer MATCHES and pt:1 has a distance while pt:2 (never visited by the ANN scan) has none."

[[test.results]]
value = "[{ id: blog:1, title: 'the quick brown fox' }]"

[[test.results]]
value = "[{ id: blog:2, title: 'the other animals sat there' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pt:1, v: [1f, 1f] }]"

[[test.results]]
value = "[{ id: pt:2, v: [8f, 8f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: blog:2, score: 0f }]"

[[test.results]]
value = "[{ dist: 0f, id: pt:1 }]"

[[test.results]]
value = "[{ id: blog:2, inner: [0f, 0f] }]"

[[test.results]]
value = "[{ id: pt:1, inner: [0f, NONE] }]"
*/

CREATE blog:1 SET title = 'the quick brown fox';
CREATE blog:2 SET title = 'the other animals sat there';
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX blog_title ON blog FIELDS title FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;

CREATE pt:1 SET v = [1f,1f];
CREATE pt:2 SET v = [8f,8f];
DEFINE INDEX pt_v ON pt FIELDS v HNSW DIMENSION 2 DIST EUCLIDEAN;

-- Baseline: the index functions resolve against the scope their own WHERE declares.
SELECT id, search::score(1) AS score FROM blog WHERE title @1@ 'animals';
SELECT id, vector::distance::knn() AS dist FROM pt WHERE v <|1,40|> [1f,1f];

-- The inner SELECT declares neither MATCHES nor a KNN operator. It must still
-- plan, resolving each index function against the enclosing SELECT's scope.
SELECT id, (SELECT VALUE search::score(1) FROM blog) AS inner FROM blog WHERE title @1@ 'animals';
SELECT id, (SELECT VALUE vector::distance::knn() FROM pt) AS inner FROM pt WHERE v <|1,40|> [1f,1f];
