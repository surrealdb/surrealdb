/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: txn:1, amount: 10, org: 'a' }]"

[[test.results]]
value = "[{ id: txn:2, amount: 20, org: 'a' }]"

[[test.results]]
value = "[{ id: txn:3, amount: 5, org: 'b' }]"

[[test.results]]
value = "[{ id: txn:4, amount: 15, org: 'b' }]"

*/
DEFINE TABLE txn SCHEMALESS;
DEFINE TABLE org_totals AS
    SELECT org, math::sum(amount) AS total, count() AS n
    FROM txn
    GROUP BY org;
CREATE txn:1 SET org = 'a', amount = 10;
CREATE txn:2 SET org = 'a', amount = 20;
CREATE txn:3 SET org = 'b', amount = 5;
CREATE txn:4 SET org = 'b', amount = 15;
