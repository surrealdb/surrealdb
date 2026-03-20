/**
[test]
reason = "Test ALTER API with all variations"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: { "/mytest": "DEFINE API '/mytest' FOR any PERMISSIONS FULL THEN { RETURN 'fallback' } FOR get PERMISSIONS FULL THEN { RETURN 'get handler' } FOR post, put PERMISSIONS FULL THEN { RETURN 'write handler' }" }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: { "/mytest": "DEFINE API '/mytest' FOR any PERMISSIONS FULL THEN { RETURN 'fallback' } FOR get PERMISSIONS FULL THEN { RETURN 'get handler' } FOR post, put PERMISSIONS FULL THEN { RETURN 'write handler' } COMMENT 'updated api'" }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: { "/mytest": "DEFINE API '/mytest' FOR any PERMISSIONS FULL THEN { RETURN 'fallback' } FOR get PERMISSIONS FULL THEN { RETURN 'get handler' } FOR post, put PERMISSIONS FULL THEN { RETURN 'write handler' }" }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: { "/mytest": "DEFINE API '/mytest' FOR any PERMISSIONS FULL FOR get PERMISSIONS FULL THEN { RETURN 'get handler' } FOR post, put PERMISSIONS FULL THEN { RETURN 'write handler' }" }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: { "/mytest": "DEFINE API '/mytest' FOR any PERMISSIONS FULL THEN { RETURN 'new fallback' } FOR get PERMISSIONS FULL THEN { RETURN 'get handler' } FOR post, put PERMISSIONS FULL THEN { RETURN 'write handler' }" }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: { "/mytest": "DEFINE API '/mytest' FOR any PERMISSIONS FULL THEN { RETURN 'new fallback' } FOR post, put PERMISSIONS FULL THEN { RETURN 'write handler' } FOR get PERMISSIONS FULL THEN { RETURN 'new get handler' }" }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: { "/mytest": "DEFINE API '/mytest' FOR any PERMISSIONS FULL THEN { RETURN 'new fallback' } FOR post, put PERMISSIONS FULL THEN { RETURN 'write handler' }" }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: { "/mytest": "DEFINE API '/mytest' FOR any PERMISSIONS FULL THEN { RETURN 'new fallback' } FOR put PERMISSIONS FULL THEN { RETURN 'write handler' }" }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: { "/mytest": "DEFINE API '/mytest' FOR any PERMISSIONS FULL THEN { RETURN 'new fallback' } FOR get PERMISSIONS FULL THEN { RETURN 'restored get' } FOR delete PERMISSIONS FULL THEN { RETURN 'delete handler' }" }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "The api '/nonexistent' does not exist"
*/

-- Setup: define an API route with a fallback and method-specific handlers
DEFINE API "/mytest" FOR any THEN { RETURN "fallback"; } FOR get THEN { RETURN "get handler"; } FOR post, put THEN { RETURN "write handler"; };
INFO FOR DB;

-- Alter: add comment
ALTER API "/mytest" COMMENT "updated api";
INFO FOR DB;

-- Alter: drop comment
ALTER API "/mytest" DROP COMMENT;
INFO FOR DB;

-- Alter: drop fallback via FOR any DROP THEN
ALTER API "/mytest" FOR any DROP THEN;
INFO FOR DB;

-- Alter: set a new fallback via FOR any THEN
ALTER API "/mytest" FOR any THEN { RETURN "new fallback"; };
INFO FOR DB;

-- Alter: set a new method-specific handler
ALTER API "/mytest" FOR get THEN { RETURN "new get handler"; };
INFO FOR DB;

-- Alter: drop a method-specific handler
ALTER API "/mytest" FOR get DROP THEN;
INFO FOR DB;

-- Alter: selective drop from a multi-method group (drop post, keep put)
ALTER API "/mytest" FOR post DROP THEN;
INFO FOR DB;

-- Alter: multiple FOR clauses in a single ALTER (add get back, drop put, add delete)
ALTER API "/mytest" FOR get THEN { RETURN "restored get"; } FOR put DROP THEN FOR delete THEN { RETURN "delete handler"; };
INFO FOR DB;

-- IF EXISTS on non-existent api
ALTER API IF EXISTS "/nonexistent" COMMENT "nope";

-- Non-existent api errors
ALTER API "/nonexistent" COMMENT "nope";
