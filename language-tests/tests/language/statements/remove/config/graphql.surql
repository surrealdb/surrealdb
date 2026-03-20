/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Test REMOVE CONFIG GRAPHQL removes an existing GraphQL config"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ graphql: { functions: 'AUTO', tables: 'AUTO' } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/

-- 0: Define a GraphQL config
DEFINE CONFIG GRAPHQL AUTO;

-- 1: Verify it exists
(INFO FOR DB STRUCTURE).configs;

-- 2: Remove the config
REMOVE CONFIG GRAPHQL;

-- 3: Verify it is gone
(INFO FOR DB STRUCTURE).configs;
