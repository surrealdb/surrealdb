/**
[test]

[[test.results]]
value = "[{ content: { content: 'Be Bop', tags: ['Foo', 'Bãr'], title: 'Hello World!' }, id: blog:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { plan: { index: 'blog_content', operator: '@1@', value: 'Hello Bãr' }, table: 'blog' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ content: ['Be Bop', 'Foo', '<em>Bãr</em>', '<em>Hello</em> World!'], id: blog:1 }]"

*/
CREATE blog:1 SET content = {{ 'title':'Hello World!', 'content':'Be Bop', 'tags': ['Foo', 'Bãr'] }};
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX blog_content ON blog FIELDS content SEARCH ANALYZER simple BM25 HIGHLIGHTS;
SELECT id FROM blog WHERE content @1@ 'Hello Bãr' EXPLAIN;
SELECT id, search::highlight('<em>', '</em>', 1) AS content FROM blog WHERE content @1@ 'Hello Bãr';
