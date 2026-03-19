/**
[test]
reason = "Test ALTER API with all variations"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: { "/mytest": "DEFINE API '/mytest' FOR any PERMISSIONS FULL THEN { RETURN 'original' }" }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
error = "The api '/'/mytest'' does not exist"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: { "/mytest": "DEFINE API '/mytest' FOR any PERMISSIONS FULL THEN { RETURN 'original' }" }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
error = "The api '/'/mytest'' does not exist"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: { "/mytest": "DEFINE API '/mytest' FOR any PERMISSIONS FULL THEN { RETURN 'original' }" }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "The api '/'/nonexistent'' does not exist"

*/

-- Setup: define an API route
DEFINE API "/mytest" FOR any THEN { RETURN "original"; };
INFO FOR DB;

-- Alter: add comment
ALTER API "/mytest" COMMENT "updated api";
INFO FOR DB;

-- Alter: drop comment
ALTER API "/mytest" DROP COMMENT;
INFO FOR DB;

-- IF EXISTS on non-existent api
ALTER API IF EXISTS "/nonexistent" COMMENT "nope";

-- Non-existent api errors
ALTER API "/nonexistent" COMMENT "nope";
