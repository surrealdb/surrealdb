/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test fulltext search with analyzer that has filters but no tokenizer"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, text: 'ab' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, text: 'ab' }]"
*/

DEFINE ANALYZER az FILTERS lowercase,ngram(1,5);
CREATE t:1 SET text = 'ab';
DEFINE INDEX search_idx ON TABLE t COLUMNS text FULLTEXT ANALYZER az BM25 HIGHLIGHTS;
SELECT * FROM t WHERE text @@ 'a';
