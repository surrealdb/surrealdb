/**
[test]
reason = "Test search::score() with BM25 scoring and fulltext index (new executor)"
*/

CREATE blog:1 SET title = 'the quick brown fox jumped over the lazy dog';
CREATE blog:2 SET title = 'the fast fox jumped over the lazy dog';
CREATE blog:3 SET title = 'the other animals sat there watching';
CREATE blog:4 SET title = 'the dog sat there and did nothing';
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX blog_title ON blog FIELDS title FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
SELECT id,search::score(1) AS score FROM blog WHERE title @1@ 'animals';
