/**
[test]

[[test.results]]
value = "[{ id: blog:1, title: 'Hello World!' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"Project [ctx: Db]\n└────> Filter [ctx: Db] [predicate: title @1@ 'Hello']\n       └────> Scan [ctx: Db] [source: blog]\n"'''

[[test.results]]
value = "[{ id: blog:1, title: '<em>Hello</em> World!' }]"

*/
CREATE blog:1 SET title = 'Hello World!';
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX blog_title ON blog FIELDS title FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
EXPLAIN SELECT id FROM blog WHERE title @1@ 'Hello';
SELECT id, search::highlight('<em>', '</em>', 1) AS title FROM blog WHERE title @1@ 'Hello';
