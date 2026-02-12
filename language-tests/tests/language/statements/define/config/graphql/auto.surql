/**
[test]
reason = "Test DEFINE CONFIG GRAPHQL AUTO sets both tables and functions to AUTO"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS AUTO' }"

[[test.results]]
value = "[{ graphql: { functions: 'AUTO', tables: 'AUTO' } }]"

[env]
namespace = true
database = true
auth = { level = "owner" }
*/

-- 0: Define GraphQL config with AUTO (sets both tables and functions to AUTO)
DEFINE CONFIG GRAPHQL AUTO;

-- 1: Verify the config formatting
(INFO FOR DB).configs;

-- 2: Verify the structured config values
(INFO FOR DB STRUCTURE).configs;
