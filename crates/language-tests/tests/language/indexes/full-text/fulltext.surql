/**
[test]

[[test.results]]
value = "[{ id: b:1, t: 'Hello World!' }]"

[[test.results]]
value = "None"

[[test.results]]
value = "None"

[[test.results]]
value = "None"

[[test.results]]
value = "{}"

[[test.results]]
value = "{}"

*/

CREATE b:1 SET t = 'Hello World!';
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX i ON b FIELDS t FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
INFO FOR TB b;
SELECT id FROM b WHERE t @1@ 'Hello' EXPLAIN;
SELECT search::highlight('{', '}', 1) AS title, search::score(1) AS score FROM b WHERE t @1@ 'Hello';