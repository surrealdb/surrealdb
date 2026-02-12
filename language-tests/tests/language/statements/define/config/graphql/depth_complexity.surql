/**
[test]
reason = "Test DEFINE CONFIG GRAPHQL with DEPTH and COMPLEXITY limits"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS AUTO DEPTH 20 COMPLEXITY 1000' }"

[[test.results]]
value = "[{ graphql: { complexity_limit: 1000, depth_limit: 20, functions: 'AUTO', tables: 'AUTO' } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS AUTO DEPTH 5' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS AUTO COMPLEXITY 500' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS AUTO' }"

[env]
namespace = true
database = true
auth = { level = "owner" }
*/

-- 0: Define GraphQL config with both DEPTH and COMPLEXITY limits
DEFINE CONFIG GRAPHQL AUTO DEPTH 20 COMPLEXITY 1000;

-- 1: Verify the config formatting includes DEPTH and COMPLEXITY
(INFO FOR DB).configs;

-- 2: Verify the structured config values include depth_limit and complexity_limit
(INFO FOR DB STRUCTURE).configs;

-- 3: Overwrite with only DEPTH limit
DEFINE CONFIG OVERWRITE GRAPHQL AUTO DEPTH 5;

-- 4: Verify only DEPTH is present
(INFO FOR DB).configs;

-- 5: Overwrite with only COMPLEXITY limit
DEFINE CONFIG OVERWRITE GRAPHQL AUTO COMPLEXITY 500;

-- 6: Verify only COMPLEXITY is present
(INFO FOR DB).configs;

-- 7: Overwrite with no limits
DEFINE CONFIG OVERWRITE GRAPHQL AUTO;

-- 8: Verify neither limit is present
(INFO FOR DB).configs;
