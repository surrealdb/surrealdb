/**
[env]
new-planner-strategy = "compute-only"
timeout = 30000
context-timeout = 30000

[test]
reason = "Test COUNT() GROUP ALL with and without index optimization"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """[{ detail: { direction: 'forward', table: 'indexPerformance3' }, operation: 'Iterate Table' }, { detail: { reason: 'WITH NOINDEX' }, operation: 'Fallback' }, { detail: { "Aggregate expressions": {  }, Aggregations: { _a0: 'Count' }, "Group expressions": {  }, "Select expression": { count: '_a0' }, type: 'Group' }, operation: 'Collector' }]"""

[[test.results]]
value = """[{ detail: { plan: { direction: 'forward', from: { inclusive: true, value: 5000 }, index: 'somethingIndex', to: { inclusive: false, value: NONE } }, table: 'indexPerformance3' }, operation: 'Iterate Index Count' }, { detail: { "Aggregate expressions": {  }, Aggregations: { _a0: 'Count' }, "Group expressions": {  }, "Select expression": { count: '_a0' }, type: 'Group' }, operation: 'Collector' }]"""

[[test.results]]
value = "[{ count: 5000 }]"

[[test.results]]
value = "[{ count: 5000 }]"
*/

FOR $i IN 0..10000 {
	CREATE indexPerformance3 CONTENT {
		something: $i
	};
};
DEFINE INDEX somethingIndex ON TABLE indexPerformance3 COLUMNS something;
SELECT count() FROM indexPerformance3 WITH NOINDEX WHERE something >= 5000 GROUP ALL EXPLAIN;
SELECT count() FROM indexPerformance3 WHERE something >= 5000 GROUP ALL EXPLAIN;
SELECT count() FROM indexPerformance3 WITH NOINDEX WHERE something >= 5000 GROUP ALL;
SELECT count() FROM indexPerformance3 WHERE something >= 5000 GROUP ALL;
