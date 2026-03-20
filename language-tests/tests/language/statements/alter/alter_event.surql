/**
[test]
reason = "Test ALTER EVENT with all variations"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ events: { my_event: "DEFINE EVENT my_event ON test WHEN $event = 'CREATE' THEN { RETURN 'original' }" }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ events: { my_event: "DEFINE EVENT my_event ON test ASYNC RETRY 3 MAXDEPTH 5 WHEN $event = 'UPDATE' THEN { RETURN 'altered' } COMMENT 'test comment'" }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ events: { my_event: "DEFINE EVENT my_event ON test WHEN $event = 'CREATE' THEN { RETURN 'restored' }" }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "The event 'nonexistent' does not exist"

*/

-- Setup: define a table and event
DEFINE TABLE test;
DEFINE EVENT my_event ON test WHEN $event = "CREATE" THEN { RETURN "original"; };
INFO FOR TB test;

-- Alter: set all properties
ALTER EVENT my_event ON test
	WHEN $event = "UPDATE"
	THEN { RETURN "altered"; }
	COMMENT "test comment"
	ASYNC RETRY 3 MAXDEPTH 5;
INFO FOR TB test;

-- Alter: drop properties back
ALTER EVENT my_event ON test
	WHEN $event = "CREATE"
	THEN { RETURN "restored"; }
	DROP COMMENT
	DROP ASYNC;
INFO FOR TB test;

-- IF EXISTS on non-existent event succeeds silently
ALTER EVENT IF EXISTS nonexistent ON test COMMENT "nope";

-- Without IF EXISTS, non-existent event errors
ALTER EVENT nonexistent ON test COMMENT "nope";
