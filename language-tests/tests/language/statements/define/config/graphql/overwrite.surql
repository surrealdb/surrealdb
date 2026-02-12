/**
[test]
reason = "Test DEFINE CONFIG OVERWRITE GRAPHQL replaces existing config, and IF NOT EXISTS preserves it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ graphql: { functions: 'AUTO', tables: 'AUTO' } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ graphql: { functions: NONE, tables: NONE } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ graphql: { functions: NONE, tables: NONE } }]"

[[test.results]]
error = "The config for graphql already exists"

[env]
namespace = true
database = true
auth = { level = "owner" }
*/

-- 0: Define initial GraphQL config with AUTO
DEFINE CONFIG GRAPHQL AUTO;

-- 1: Verify initial structured config
(INFO FOR DB STRUCTURE).configs;

-- 2: Overwrite with a different config (TABLES NONE, FUNCTIONS NONE)
DEFINE CONFIG OVERWRITE GRAPHQL NONE;

-- 3: Verify config was overwritten
(INFO FOR DB STRUCTURE).configs;

-- 4: IF NOT EXISTS should not overwrite the existing config
DEFINE CONFIG IF NOT EXISTS GRAPHQL AUTO;

-- 5: Verify config is still NONE (not overwritten by IF NOT EXISTS)
(INFO FOR DB STRUCTURE).configs;

-- 6: Re-define without OVERWRITE or IF NOT EXISTS should error
DEFINE CONFIG GRAPHQL AUTO;
