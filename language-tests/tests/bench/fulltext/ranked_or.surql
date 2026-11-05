/**
[env]
imports = ["bench/util/docs-100000-ranked-fulltext.surql"]

[bench]
reason = "Top 10 by summed BM25 score of two clauses combined with OR (w5, w20)"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, search::score(1) + search::score(2) AS s FROM doc WHERE body @1@ 'w5' OR body @2@ 'w20' ORDER BY s DESC LIMIT 10;
