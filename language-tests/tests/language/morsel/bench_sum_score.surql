/**
[env]
imports = ["bench/util/records-100000.surql"]
timeout = 180000

[test]
reason = "the statement of the bench sum_score_100k, over the dataset the bench reads, answers one row, and the fold of its morsels is the fold of the scores in the order a scan reads them"

[[test.results]]
match = "array::len($result) == 1 AND type::is_float($result[0].`math::sum`)"
error = false

[[test.results]]
value = "true"
*/

SELECT math::sum(score) FROM record GROUP ALL;
RETURN (SELECT math::sum(score) AS total FROM record GROUP ALL)[0].total == math::sum((SELECT VALUE score FROM record));
