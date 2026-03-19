/**
[test]
reason = "Test ALTER CONFIG with all variations"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: { GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS AUTO' }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: { GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS AUTO' }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"

[[test.results]]
value = "NONE"

*/

-- Setup: define a graphql config
DEFINE CONFIG GRAPHQL AUTO;
INFO FOR DB;

-- Alter: update config
ALTER CONFIG GRAPHQL TABLES AUTO FUNCTIONS AUTO;
INFO FOR DB;

-- Alter: with IF EXISTS on non-existent (defaults config)
ALTER CONFIG IF EXISTS DEFAULT NAMESPACE test;
