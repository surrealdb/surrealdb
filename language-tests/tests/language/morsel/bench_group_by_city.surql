/**
[env]
imports = ["bench/util/records-100000.surql"]
timeout = 180000

[test]
reason = "the statement of the bench aggregate_group_by_city_100k, over the dataset the bench reads, answers a group for each city, in the order of their names, and counts every one of the 100,000 records once"

[[test.results]]
match = "array::len($result) == 9 AND math::sum($result.count) == 100000 AND $result.city == array::sort($result.city)"
error = false
*/

SELECT city, count() FROM record GROUP BY city;
