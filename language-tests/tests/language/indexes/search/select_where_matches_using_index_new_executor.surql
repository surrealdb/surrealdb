/**
[test]

[[test.results]]
value = "[{ id: blog:1, title: 'Hello World!' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1}
└────> Filter [ctx: Db] [predicate: title @1@ 'Hello'] {rows: 1}
       └────> FullTextScan [ctx: Db] [index: blog_title, query: Hello] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ id: blog:1, title: '<em>Hello</em> World!' }]"

*/
CREATE blog:1 SET title = 'Hello World!';
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX blog_title ON blog FIELDS title FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
EXPLAIN ANALYZE SELECT id FROM blog WHERE title @1@ 'Hello';
SELECT id, search::highlight('<em>', '</em>', 1) AS title FROM blog WHERE title @1@ 'Hello';
