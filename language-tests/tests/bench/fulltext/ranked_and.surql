/**
[env]
imports = ["bench/util/docs-100000-ranked-fulltext.surql"]

[bench]
reason = "Top 10 by BM25 score of two terms that must both match (w5 and w20)"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, search::score(1) AS s FROM doc WHERE body @1@ 'w5 w20' ORDER BY s DESC LIMIT 10;
