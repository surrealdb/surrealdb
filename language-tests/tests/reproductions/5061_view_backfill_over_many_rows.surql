/**
[env]
timeout = 60000

[test]
reason = "Defining a grouped and a flat view over a table that already holds many rows fills both from the rows within the statement, well inside the test's timeout, and the views hold every group and every admitted row."
issue = 5061

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 5000, s: 0 }, { c: 5000, s: 1 }, { c: 5000, s: 2 }, { c: 5000, s: 3 }]"

[[test.results]]
value = "10000"

*/

DEFINE TABLE obs;
FOR $i IN 0..20000 { CREATE obs SET s = $i % 4, v = $i RETURN NONE };
DEFINE TABLE obs_by_s AS SELECT count() AS c, math::mean(v) AS m, s FROM obs GROUP BY s;
DEFINE TABLE obs_even AS SELECT v, s FROM obs WHERE v % 2 = 0;
SELECT c, s FROM obs_by_s ORDER BY s;
count(SELECT VALUE id FROM obs_even);
