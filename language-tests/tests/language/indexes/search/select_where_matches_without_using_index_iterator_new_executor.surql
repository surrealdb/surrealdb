/**
[test]

[[test.results]]
value = "[{ id: blog:1, title: 'Hello World!' }]"

[[test.results]]
value = "[{ id: blog:2, title: 'Foo Bar!' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: New executor does not support: WHERE clause with MATCHES in OR conditions not yet supported in streaming executor"

[[test.results]]
error = "Invalid query: New executor does not support: WHERE clause with MATCHES in OR conditions not yet supported in streaming executor"

*/
CREATE blog:1 SET title = 'Hello World!';
CREATE blog:2 SET title = 'Foo Bar!';
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX blog_title ON blog FIELDS title FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
EXPLAIN SELECT id FROM blog WHERE (title @0@ 'hello' AND identifier > 0) OR (title @1@ 'world' AND identifier < 99);
SELECT id,search::highlight('<em>', '</em>', 1) AS title FROM blog WHERE (title @0@ 'hello' AND identifier > 0) OR (title @1@ 'world' AND identifier < 99);
