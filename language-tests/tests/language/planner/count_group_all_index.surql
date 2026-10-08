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
SELECT count() FROM indexPerformance3 WITH NOINDEX WHERE something >= 5000 GROUP ALL;
SELECT count() FROM indexPerformance3 WHERE something >= 5000 GROUP ALL;
