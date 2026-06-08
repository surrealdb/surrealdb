/**
[env]
imports = ["upgrade/define/table_view_aggregate_import.surql"]

[test]
upgrade = true
reason = "Aggregating view with GROUP BY (AggregationAnalysis state in catalog/view.rs) survives upgrade — pre-aggregated counts and sums are preserved and new inserts continue to update the aggregates correctly"

[[test.results]]
value = "[{ id: org_totals:['a'], n: 2, org: 'a', total: 30f }, { id: org_totals:['b'], n: 2, org: 'b', total: 20f }]"

[[test.results]]
value = "[{ id: txn:5, amount: 100, org: 'a' }]"

[[test.results]]
value = "[{ id: org_totals:['a'], n: 3, org: 'a', total: 130f }, { id: org_totals:['b'], n: 2, org: 'b', total: 20f }]"

*/
SELECT * FROM org_totals ORDER BY org;
CREATE txn:5 SET org = 'a', amount = 100;
SELECT * FROM org_totals ORDER BY org;
