/**
[env]
imports = ["bench/util/records-100000.surql"]
timeout = 120000

[test]
reason = "the statement of the bench select_all_100k, over the dataset the bench reads, answers every one of its 100,000 records once, whatever order its morsels finish in"

[[test.results]]
match = "array::len($result) == 100000 AND array::len(array::distinct($result.id)) == 100000"
error = false

[[test.results]]
value = "[{ count: 100000 }]"
*/

SELECT * FROM record;
SELECT count() FROM record GROUP ALL;
