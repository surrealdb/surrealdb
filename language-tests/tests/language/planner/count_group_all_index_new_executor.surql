/**
[env]

timeout = 30000
context-timeout = 30000

[test]
reason = "Test COUNT() GROUP ALL with and without index optimization"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *] {rows: 1}
    Aggregate [ctx: Db] [mode: GROUP ALL] {rows: 1}
        TableScan [ctx: Db] [table: indexPerformance3, direction: Forward, predicate: something >= 5000, pre_decode_filter: yes] {rows: 1}

Total rows: 1''''

[[test.results]]
value = "[{ count: 5000 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *] {rows: 1}
    IndexCountScan [ctx: Db] [source: indexPerformance3, condition: something >= 5000] {rows: 1}

Total rows: 1''''

[[test.results]]
value = "[{ count: 5000 }]"
*/

-- Index keys are counted only for a column declared to hold no array.
DEFINE FIELD something ON indexPerformance3 TYPE int;
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
