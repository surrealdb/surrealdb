/**
[env]
imports = ["bench/util/records-100000.surql"]
timeout = 180000

[test]
reason = "the statement of the bench order_by_score_100k, over the dataset the bench reads, answers every one of its 100,000 records once, ordered by score"

[[test.results]]
match = "array::len($result) == 100000 AND array::len(array::distinct($result.id)) == 100000 AND $result.score == array::sort($result.score)"
error = false
*/

SELECT id, score FROM record ORDER BY score;
