/**
[test]
reason = "vector::distance::knn() returns 0 when using integer types (I64/I32/I16) instead of float types"
issue = 6586

[[test.results]]
value = "[{ embedding: [1f, 2f, 3f], id: test:1, text: 'First document' }]"

[[test.results]]
value = "[{ embedding: [10f, 20f, 30f], id: test:2, text: 'Second document' }]"

[[test.results]]
value = "[{ embedding: [100f, 200f, 300f], id: test:3, text: 'Third document' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# Float embeddings should work (control case)
[[test.results]]
match = "$result[0].distance > 0"
error = false

# Integer table creates
[[test.results]]
value = "[{ embedding: [1, 2, 3], id: test_int:1, text: 'First document' }]"

[[test.results]]
value = "[{ embedding: [10, 20, 30], id: test_int:2, text: 'Second document' }]"

[[test.results]]
value = "[{ embedding: [100, 200, 300], id: test_int:3, text: 'Third document' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# Integer embeddings should also return non-zero distances
[[test.results]]
match = "$result[0].distance > 0"
error = false

*/

-- Float embeddings (control case - this works)
CREATE test:1 SET text = "First document", embedding = [1.0, 2.0, 3.0];
CREATE test:2 SET text = "Second document", embedding = [10.0, 20.0, 30.0];
CREATE test:3 SET text = "Third document", embedding = [100.0, 200.0, 300.0];

DEFINE INDEX idx_float ON TABLE test FIELDS embedding HNSW DIMENSION 3 DIST EUCLIDEAN;

LET $qvec_float = [5.0, 10.0, 15.0];

-- This should return non-zero distances (float case)
SELECT id, vector::distance::knn() as distance FROM test WHERE embedding <|2,100|> $qvec_float;

-- Now test with integer embeddings
CREATE test_int:1 SET text = "First document", embedding = [1, 2, 3];
CREATE test_int:2 SET text = "Second document", embedding = [10, 20, 30];
CREATE test_int:3 SET text = "Third document", embedding = [100, 200, 300];

DEFINE INDEX idx_int ON TABLE test_int FIELDS embedding HNSW DIMENSION 3 DIST EUCLIDEAN;

LET $qvec_int = [5, 10, 15];

-- This returns 0 distances (bug) - should return non-zero
SELECT id, vector::distance::knn() as distance FROM test_int WHERE embedding <|2,100|> $qvec_int;
