/**
[test]
reason = "Test ALTER ANALYZER with all variations"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: { my_analyzer: 'DEFINE ANALYZER my_analyzer TOKENIZERS BLANK' }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: { my_analyzer: "DEFINE ANALYZER my_analyzer TOKENIZERS CLASS FILTERS ASCII COMMENT 'updated analyzer'" }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: { my_analyzer: 'DEFINE ANALYZER my_analyzer' }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The analyzer 'nonexistent' does not exist"

*/

-- Setup
DEFINE ANALYZER my_analyzer TOKENIZERS blank;
INFO FOR DB;

-- Alter: set tokenizers, filters, and comment
ALTER ANALYZER my_analyzer
	TOKENIZERS class
	FILTERS ascii
	COMMENT "updated analyzer";
INFO FOR DB;

-- Alter: drop all optional properties
ALTER ANALYZER my_analyzer
	DROP TOKENIZERS
	DROP FILTERS
	DROP COMMENT;
INFO FOR DB;

-- IF EXISTS on non-existent analyzer
ALTER ANALYZER IF EXISTS nonexistent COMMENT "nope";

-- Non-existent analyzer errors
ALTER ANALYZER nonexistent COMMENT "nope";
