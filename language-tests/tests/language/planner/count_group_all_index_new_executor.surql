/**
[env]
timeout = 30000
context-timeout = 30000

[test]
reason = "Test COUNT() GROUP ALL with and without index optimization (new executor)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = ''''Aggregate [ctx: Db] [mode: GROUP ALL] {rows: 1, batches: 1}
└────> TableScan [ctx: Db] [table: indexPerformance3, direction: Forward, predicate: something >= 5000] {rows: 5000, batches: 2}

Total rows: 1''''

[[test.results]]
value = "[{ count: 5000 }]"

[[test.results]]
value = ''''Aggregate [ctx: Db] [mode: GROUP ALL] {rows: 1, batches: 1}
└────> Filter [ctx: Db] [predicate: something >= 5000] {rows: 5000, batches: 5}
       └────> IndexScan [ctx: Db] [index: somethingIndex, access: >=5000, direction: Forward] {rows: 5000, batches: 5}

Total rows: 1''''

[[test.results]]
value = "[{ count: 5000 }]"
*/

FOR $i IN 0..10000 {
	CREATE indexPerformance3 CONTENT {
		something: $i
	};
};
DEFINE INDEX somethingIndex ON TABLE indexPerformance3 COLUMNS something;
EXPLAIN ANALYZE SELECT count() FROM indexPerformance3 WITH NOINDEX WHERE something >= 5000 GROUP ALL;
SELECT count() FROM indexPerformance3 WITH NOINDEX WHERE something >= 5000 GROUP ALL;
EXPLAIN ANALYZE SELECT count() FROM indexPerformance3 WHERE something >= 5000 GROUP ALL;
SELECT count() FROM indexPerformance3 WHERE something >= 5000 GROUP ALL;
