/**
[test]

[[test.results]]
value = "[{ id: b:1, t: 'Hello World!' }]"

[[test.results]]
value = "None"

[[test.results]]
value = "None"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { i: 'DEFINE INDEX i ON b FIELDS t FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ detail: { plan: { index: 'i', operator: '@1@', value: 'Hello' }, table: 'b' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ score: NaN, title: '{Hello} World!' }]"

[[test.results]]
value = "[{ id: b:2, t: 'Foo Bar' }]"

[[test.results]]
value = "[{ score: NaN, title: 'Foo {Bar}' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ score: NaN, title: 'Foo Bar' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

CREATE b:1 SET t = 'Hello World!';
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX i ON b FIELDS t FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
INFO FOR TB b;
SELECT id FROM b WHERE t @1@ 'Hello' EXPLAIN;
SELECT search::highlight('{', '}', 1) AS title, search::score(1) AS score FROM b WHERE t @1@ 'Hello';
CREATE b:2 SET t = 'Foo Bar';
SELECT search::highlight('{', '}', 1) AS title, search::score(1) AS score FROM b WHERE t @1@ 'Bar';
DELETE b:1;
SELECT search::highlight('{', '}', 1) AS title, search::score(1) * search::score(2) AS score FROM b WHERE t @1@ 'Hello' OR t @2@ 'Bar';
DELETE b:2;
SELECT search::highlight('{', '}', 1) AS title, search::score(1) * search::score(2) AS score FROM b WHERE t @1@ 'Hello' OR t @2@ 'Bar';