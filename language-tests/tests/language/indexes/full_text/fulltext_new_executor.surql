/**
[test]

[[test.results]]
value = "[{ id: b:1, t: 'Hello World!' }]"

[[test.results]]
value = "[{ id: b:2, t: 'Yet Another World!' }]"

[[test.results]]
value = "[{ id: b:3, t: 'again hello' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { i: 'DEFINE INDEX i ON b FIELDS t FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS' }, lives: {  }, tables: {  } }"

[[test.results]]
value = '''
"Project [ctx: Db]
└────> Scan [ctx: Db] [source: b, predicate: t @1@ 'Hello']
"'''

[[test.results]]
value = "[{ score: 0.5108256340026855f, title: '{Hello} World!' }]"

[[test.results]]
value = "[{ id: b:4, t: 'Foo Bar' }]"

[[test.results]]
value = '[{ score: 0.9537026882171631f, title: { "0": [{ e: 7, s: 4 }] } }]'

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ score: 0, title: 'Foo Bar' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

CREATE b:1 SET t = 'Hello World!';
CREATE b:2 SET t = 'Yet Another World!';
CREATE b:3 SET t = 'again hello';
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX i ON b FIELDS t FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
INFO FOR TB b;
EXPLAIN SELECT id FROM b WHERE t @1@ 'Hello';
SELECT search::highlight('{', '}', 1) AS title, search::score(1) AS score FROM b WHERE t @1@ 'Hello';
CREATE b:4 SET t = 'Foo Bar';
SELECT search::offsets(1) AS title, search::score(1) AS score FROM b WHERE t @1@ 'Bar';
DELETE b:1;
SELECT search::highlight('{', '}', 1) AS title, search::score(1) * search::score(2) AS score FROM b WHERE t @1@ 'Hello' OR t @2@ 'Bar';
DELETE b:4;
SELECT search::highlight('{', '}', 1) AS title, search::score(1) * search::score(2) AS score FROM b WHERE t @1@ 'Hello' OR t @2@ 'Bar';
