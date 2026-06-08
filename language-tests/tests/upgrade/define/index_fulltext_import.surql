/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: blog:1, label: 'test', title: 'the quick brown fox jumped over the lazy dog' }]"

[[test.results]]
value = "[{ id: blog:2, label: 'test', title: 'the fast fox jumped over the lazy dog' }]"

[[test.results]]
value = "[{ id: blog:3, label: 'test', title: 'the other animals sat there watching' }]"

[[test.results]]
value = "[{ id: blog:4, label: 'test', title: 'the dog sat there and did nothing' }]"

*/
DEFINE ANALYZER simple TOKENIZERS blank, class FILTERS lowercase;
DEFINE INDEX blog_title ON blog FIELDS title FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
CREATE blog:1 SET title = 'the quick brown fox jumped over the lazy dog', label = 'test';
CREATE blog:2 SET title = 'the fast fox jumped over the lazy dog', label = 'test';
CREATE blog:3 SET title = 'the other animals sat there watching', label = 'test';
CREATE blog:4 SET title = 'the dog sat there and did nothing', label = 'test';
