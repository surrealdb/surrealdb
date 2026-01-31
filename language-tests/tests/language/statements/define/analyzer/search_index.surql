/**
[env]
timeout = 10000
context-timeout = 10000

[test]

[[test.results]]
value = "[{ id: blog:1, title: 'Understanding SurrealQL and how it is different from PostgreSQL' }]"

[[test.results]]
value = "[{ id: blog:3, title: 'This blog is going to be deleted' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: blog:2, title: 'Behind the scenes of the exciting beta 9 release' }]"

[[test.results]]
value = "[]"


[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { blog_title: 'DEFINE INDEX blog_title ON blog FIELDS title FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS' }, lives: {  }, tables: {  } }"

*/
CREATE blog:1 SET title = 'Understanding SurrealQL and how it is different from PostgreSQL';
CREATE blog:3 SET title = 'This blog is going to be deleted';
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX blog_title ON blog FIELDS title FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
CREATE blog:2 SET title = 'Behind the scenes of the exciting beta 9 release';
DELETE blog:3;

INFO FOR TABLE blog;