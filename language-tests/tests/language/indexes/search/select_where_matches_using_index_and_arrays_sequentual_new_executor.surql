/**
[test]

[[test.results]]
value = "[{ content: ['Hello World!', 'Be Bop', 'Foo Bãr'], id: blog:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1, batches: 1}
└────> Scan [ctx: Db] [source: blog, predicate: content @1@ 'Hello Bãr'] {rows: 1, batches: 1}
         actual: [access_method: FullTextScan, index: blog_content, query: Hello Bãr]

Total rows: 1"'''

[[test.results]]
value = "[{ content: ['<em>Hello</em> World!', 'Be Bop', 'Foo <em>Bãr</em>'], id: blog:1 }]"

*/
CREATE blog:1 SET content = ['Hello World!', 'Be Bop', 'Foo Bãr'];
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX blog_content ON blog FIELDS content FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
EXPLAIN ANALYZE SELECT id FROM blog WHERE content @1@ 'Hello Bãr';
SELECT id, search::highlight('<em>', '</em>', 1) AS content FROM blog WHERE content @1@ 'Hello Bãr';
