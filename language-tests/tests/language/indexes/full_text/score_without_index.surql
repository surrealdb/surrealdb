/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test search::score() when fulltext index is not directly used as the iterator"

[[test.results]]
value = "[{ id: blog:1, label: 'test', title: 'the quick brown fox jumped over the lazy dog' }]"

[[test.results]]
value = "[{ id: blog:2, label: 'test', title: 'the fast fox jumped over the lazy dog' }]"

[[test.results]]
value = "[{ id: blog:3, label: 'test', title: 'the other animals sat there watching' }]"

[[test.results]]
value = "[{ id: blog:4, label: 'test', title: 'the dog sat there and did nothing' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: blog:3, score: 0.9227996468544006f }]"

[[test.results]]
value = "[]"
*/

CREATE blog:1 SET title = 'the quick brown fox jumped over the lazy dog', label = 'test';
CREATE blog:2 SET title = 'the fast fox jumped over the lazy dog', label = 'test';
CREATE blog:3 SET title = 'the other animals sat there watching', label = 'test';
CREATE blog:4 SET title = 'the dog sat there and did nothing', label = 'test';
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX blog_title ON blog FIELDS title FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
LET $keywords = 'animals';
SELECT id,search::score(1) AS score FROM blog
	WHERE (title @1@ $keywords AND label = 'test')
	OR (title @1@ $keywords AND label = 'test');
-- Non-existing terms should return empty
SELECT id,search::score(1) + search::score(2) AS score FROM blog
	WHERE (title @1@ 'dummy1' AND label = 'test')
	OR (title @2@ 'dummy2' AND label = 'test');
