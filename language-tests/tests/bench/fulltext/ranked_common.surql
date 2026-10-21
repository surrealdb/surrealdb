/**
[env]
imports = ["bench/util/docs-100000-ranked-fulltext.surql"]

[bench]
reason = "Top 10 by BM25 score of a common term (w5, about 21% of 100k documents)"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, search::score(1) AS s FROM doc WHERE body @1@ 'w5' ORDER BY s DESC LIMIT 10;
