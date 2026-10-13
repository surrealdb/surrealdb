/**
[test]
reason = "In an aggregated view whose columns are aliases that name each other's source columns (`g1 AS flag, g0 AS g1` grouped by `flag, g1`), each alias is the group key it names. Each group is keyed by the values of the columns its alias was projected from, and the view holds the same groups whether it is defined before its rows exist or over them, before and after a source row is deleted."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ g0: 'a', g1: true, id: gs:1 }]"

[[test.results]]
value = "[{ g0: 'a', g1: true, id: gs:2 }]"

[[test.results]]
value = "[{ g0: 'x', g1: false, id: gs:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 1, flag: false, g1: 'x', id: live_sw:[false, 'x'] }, { c: 2, flag: true, g1: 'a', id: live_sw:[true, 'a'] }]"

[[test.results]]
value = "[{ c: 1, flag: false, g1: 'x', id: late_sw:[false, 'x'] }, { c: 2, flag: true, g1: 'a', id: late_sw:[true, 'a'] }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c: 2, flag: true, g1: 'a', id: live_sw:[true, 'a'] }]"

[[test.results]]
value = "[{ c: 2, flag: true, g1: 'a', id: late_sw:[true, 'a'] }]"

[[test.results]]
value = "true"
*/
DEFINE TABLE gs SCHEMALESS;
DEFINE TABLE live_sw AS SELECT g1 AS flag, g0 AS g1, count() AS c FROM gs GROUP BY flag, g1;
CREATE gs:1 SET g0 = 'a', g1 = true;
CREATE gs:2 SET g0 = 'a', g1 = true;
CREATE gs:3 SET g0 = 'x', g1 = false;
DEFINE TABLE late_sw AS SELECT g1 AS flag, g0 AS g1, count() AS c FROM gs GROUP BY flag, g1;
SELECT * FROM live_sw ORDER BY id;
SELECT * FROM late_sw ORDER BY id;
(SELECT flag, g1, c FROM live_sw ORDER BY flag, g1) = (SELECT flag, g1, c FROM late_sw ORDER BY flag, g1);
DELETE gs:3;
SELECT * FROM live_sw ORDER BY id;
SELECT * FROM late_sw ORDER BY id;
(SELECT flag, g1, c FROM live_sw ORDER BY flag, g1) = (SELECT flag, g1, c FROM late_sw ORDER BY flag, g1);
