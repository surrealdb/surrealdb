/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test partial highlights and offsets with edgengram analyzer"

[[test.results]]
value = "[{ content: 'Hello World!', id: blog:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ content: '<em>Hello</em> World!', id: blog:1 }]"

[[test.results]]
value = "[{ content: '<em>Hello</em> World!', id: blog:1 }]"

[[test.results]]
value = "[{ content: '<em>He</em>llo World!', id: blog:1 }]"

[[test.results]]
value = '[{ content: { "0": [{ e: 5, s: 0 }] }, id: blog:1 }]'

[[test.results]]
value = '[{ content: { "0": [{ e: 5, s: 0 }] }, id: blog:1 }]'

[[test.results]]
value = '[{ content: { "0": [{ e: 2, s: 0 }] }, id: blog:1 }]'
*/

CREATE blog:1 SET content = 'Hello World!';
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase,edgengram(2,100);
DEFINE INDEX blog_content ON blog FIELDS content FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
SELECT id, search::highlight('<em>', '</em>', 1) AS content FROM blog WHERE content @1@ 'he';
SELECT id, search::highlight('<em>', '</em>', 1, false) AS content FROM blog WHERE content @1@ 'he';
SELECT id, search::highlight('<em>', '</em>', 1, true) AS content FROM blog WHERE content @1@ 'he';
SELECT id, search::offsets(1) AS content FROM blog WHERE content @1@ 'he';
SELECT id, search::offsets(1, false) AS content FROM blog WHERE content @1@ 'he';
SELECT id, search::offsets(1, true) AS content FROM blog WHERE content @1@ 'he';
