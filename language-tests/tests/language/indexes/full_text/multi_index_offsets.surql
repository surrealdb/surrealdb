/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test search offsets with multiple fulltext indexes on different fields"

[[test.results]]
value = "[{ content: ['Hello World!', 'Be Bop', 'Foo Bãr'], id: blog:1, title: 'Blog title!' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '[{ content: { "0": [{ e: 5, s: 0 }], "2": [{ e: 7, s: 4 }] }, id: blog:1, title: { "0": [{ e: 10, s: 5 }] } }]'
*/

CREATE blog:1 SET title = 'Blog title!', content = ['Hello World!', 'Be Bop', 'Foo Bãr'];
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX blog_title ON blog FIELDS title FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
DEFINE INDEX blog_content ON blog FIELDS content FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
SELECT id, search::offsets(0) AS title, search::offsets(1) AS content FROM blog WHERE title @0@ 'title' AND content @1@ 'Hello Bãr';
