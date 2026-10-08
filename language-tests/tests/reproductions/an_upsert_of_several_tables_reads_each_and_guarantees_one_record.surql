/**
[env]
namespace = true
database = true

[test]
reason = "An UPSERT of several tables under a WHERE reads each table's records in order through the access path a read takes, a table named twice read twice; when the condition matched none it writes one record, in the last table its gate admits; with a table the catalog does not hold or a record id among its targets it writes the records each target names."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: ua:1, k: 1 }]"

[[test.results]]
value = "[{ id: ub:1, k: 1 }]"

[[test.results]]
value = "[{ id: ub:2, k: 2 }]"

[[test.results]]
value = "[ua:1, ub:1]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[ub:2]"

[[test.results]]
value = "1"

[[test.results]]
value = "[ua:1]"

[[test.results]]
value = "[ua:1, ub:1]"

[[test.results]]
value = "[{ c: 5, n: 1 }]"

*/
DEFINE INDEX ia ON ua FIELDS k;
DEFINE INDEX ib ON ub FIELDS k;
CREATE ua:1 SET k = 1; CREATE ub:1 SET k = 1; CREATE ub:2 SET k = 2;
UPSERT ua, ub SET hit = true WHERE k = 1 RETURN VALUE id;
UPSERT ua, ub SET hit2 = true WHERE k = 9 RETURN VALUE k;
UPSERT ua, ub, ua SET n += 1 WHERE k = 2 RETURN VALUE id;
UPSERT ONLY ua, ub SET z = 1 WHERE k = 7 RETURN VALUE z;
UPSERT ua, missing SET q = 1 WHERE k = 1 RETURN VALUE id;
UPSERT ua, ub:1 SET r = 1 WHERE k = 1 RETURN VALUE id;
SELECT count() AS c, math::sum(n) AS n FROM ua, ub GROUP ALL;
