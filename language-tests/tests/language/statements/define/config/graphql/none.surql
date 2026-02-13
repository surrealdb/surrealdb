/**
[test]
reason = "Test DEFINE CONFIG GRAPHQL NONE and the bare form (no keywords) both default tables and functions to NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES NONE FUNCTIONS NONE' }"

[[test.results]]
value = "[{ graphql: { functions: NONE, tables: NONE } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES NONE FUNCTIONS NONE' }"

[[test.results]]
value = "[{ graphql: { functions: NONE, tables: NONE } }]"

[env]
namespace = true
database = true
auth = { level = "owner" }
*/

-- 0: Define GraphQL config with explicit NONE
DEFINE CONFIG GRAPHQL NONE;

-- 1: Verify the config formatting
(INFO FOR DB).configs;

-- 2: Verify the structured config values
(INFO FOR DB STRUCTURE).configs;

-- 3: Overwrite with bare form (no extra keywords)
DEFINE CONFIG OVERWRITE GRAPHQL;

-- 4: Verify the bare form formatting matches explicit NONE
(INFO FOR DB).configs;

-- 5: Verify the bare form structured values match explicit NONE
(INFO FOR DB STRUCTURE).configs;
