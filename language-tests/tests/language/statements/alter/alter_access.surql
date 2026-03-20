/**
[env]
auth = { level = "owner" }

[test]
reason = "Test ALTER ACCESS with all variations"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: { my_access: "DEFINE ACCESS my_access ON DATABASE TYPE JWT ALGORITHM HS256 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION 1d" }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: { my_access: "DEFINE ACCESS my_access ON DATABASE TYPE JWT ALGORITHM HS256 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 2h, FOR SESSION 2d COMMENT 'updated access'" }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: { my_access: "DEFINE ACCESS my_access ON DATABASE TYPE JWT ALGORITHM HS256 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN NONE, FOR SESSION NONE" }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "The access method 'nonexistent' does not exist in the database 'test'"

*/

-- Setup: create a JWT access method
DEFINE ACCESS my_access ON DATABASE TYPE JWT
	ALGORITHM HS256 KEY 'secret'
	DURATION FOR TOKEN 1h, FOR SESSION 24h;
INFO FOR DB;

-- Alter: change durations and comment
ALTER ACCESS my_access ON DATABASE
	DURATION FOR TOKEN 2h, FOR SESSION 48h
	COMMENT "updated access";
INFO FOR DB;

-- Alter: drop durations and comment
ALTER ACCESS my_access ON DATABASE
	DURATION FOR TOKEN NONE, FOR SESSION NONE
	DROP COMMENT;
INFO FOR DB;

-- IF EXISTS on non-existent access
ALTER ACCESS IF EXISTS nonexistent ON DATABASE COMMENT "nope";

-- Non-existent access errors
ALTER ACCESS nonexistent ON DATABASE COMMENT "nope";
