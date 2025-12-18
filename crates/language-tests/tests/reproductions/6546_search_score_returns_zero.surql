/**
[env]
backend = ["rocksdb", "surrealkv"]

[test]
reason = "search::score returns 0 after fulltext index compaction"
issue = 6546

[[test.results]]
value = "[{ id: test:1, text: 'Hello World!' }]"

[[test.results]]
value = "[{ id: test:2, text: 'Test document' }]"

[[test.results]]
value = "[{ id: test:3, text: 'Another test file' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# Before compaction: score should be non-zero
[[test.results]]
match = "$result[0].score > 0"
error = false

[[test.results]]
value = "NONE"

# After compaction: score should STILL be non-zero
[[test.results]]
match = "$result[0].score > 0"
error = false

*/

-- Create test data first (like the working test)
CREATE test:1 SET text = 'Hello World!';
CREATE test:2 SET text = 'Test document';
CREATE test:3 SET text = 'Another test file';

-- Setup analyzer and index (after creating data)
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX search_idx ON test FIELDS text FULLTEXT ANALYZER simple BM25;

-- Query score BEFORE compaction
SELECT *, search::score(1) AS score FROM test WHERE text @1@ 'test' ORDER BY score DESC;

-- Trigger compaction
ALTER TABLE test COMPACT;

-- Query score AFTER compaction (should still work)
SELECT *, search::score(1) AS score FROM test WHERE text @1@ 'test' ORDER BY score DESC;
