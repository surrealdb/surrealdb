/**
[test]
reason = """
	A write made by an ASYNC event is delivered to live queries on its table.
	Live notifications need a notification channel, which the language-test
	runner does not expose, so the behaviour is pinned by the Rust test
	`asyncevent::an_async_event_write_notifies_a_live_query` in surrealdb-core;
	this file holds the write the live query is owed.
"""
issue = 7313

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:1 }]"
*/
DEFINE TABLE test SCHEMALESS;
CREATE test:1;
SLEEP 1ms;
SELECT id FROM test;
