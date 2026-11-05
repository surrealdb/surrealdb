/**
[env]
imports = ["bench/util/docs-100000-ranked-fulltext.surql"]

[bench]
reason = "Top 10 by BM25 score of a rare term, each with its highlighted body (w500)"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, search::score(1) AS s, search::highlight('<b>', '</b>', 1) AS h FROM doc WHERE body @1@ 'w500' ORDER BY s DESC LIMIT 10;
