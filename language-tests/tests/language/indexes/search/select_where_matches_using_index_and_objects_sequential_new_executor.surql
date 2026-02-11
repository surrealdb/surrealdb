/**
[test]

[[test.results]]
value = "[{ content: { content: 'Be Bop', tags: ['Foo', 'Bãr'], title: 'Hello World!' }, id: blog:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"Project [ctx: Db]
└────> Scan [ctx: Db] [source: blog, predicate: content @1@ 'Hello Bãr']
"'''

[[test.results]]
value = "[{ content: ['Be Bop', 'Foo', '<em>Bãr</em>', '<em>Hello</em> World!'], id: blog:1 }]"

*/
CREATE blog:1 SET content = {{ 'title':'Hello World!', 'content':'Be Bop', 'tags': ['Foo', 'Bãr'] }};
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX blog_content ON blog FIELDS content FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
EXPLAIN SELECT id FROM blog WHERE content @1@ 'Hello Bãr';
SELECT id, search::highlight('<em>', '</em>', 1) AS content FROM blog WHERE content @1@ 'Hello Bãr';
