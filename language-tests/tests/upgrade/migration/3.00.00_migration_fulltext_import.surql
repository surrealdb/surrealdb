/**
[test]
reason = "Setup for migration_fulltext.surql: a RocksDB store holding a full-text index."
run = false
*/
DEFINE ANALYZER mig_simple TOKENIZERS blank, class FILTERS lowercase;
DEFINE INDEX mig_blog_title ON mig_blog FIELDS title FULLTEXT ANALYZER mig_simple BM25 HIGHLIGHTS;
CREATE mig_blog:1 SET title = 'the quick brown fox jumped over the lazy dog';
CREATE mig_blog:2 SET title = 'the fast fox jumped over the lazy dog';
CREATE mig_blog:3 SET title = 'the other animals sat there watching';
CREATE mig_blog:4 SET title = 'the dog sat there and did nothing';
