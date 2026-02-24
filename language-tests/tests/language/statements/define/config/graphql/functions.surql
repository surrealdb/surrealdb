/**
[test]
reason = "Test DEFINE CONFIG GRAPHQL FUNCTIONS AUTO and FUNCTIONS NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES NONE FUNCTIONS AUTO' }"

[[test.results]]
value = "[{ graphql: { functions: 'AUTO', tables: NONE } }]"

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

-- 0: Define GraphQL config with FUNCTIONS AUTO (tables defaults to NONE)
DEFINE CONFIG GRAPHQL FUNCTIONS AUTO;

-- 1: Verify config formatting
(INFO FOR DB).configs;

-- 2: Verify structured config values
(INFO FOR DB STRUCTURE).configs;

-- 3: Overwrite with FUNCTIONS NONE
DEFINE CONFIG OVERWRITE GRAPHQL FUNCTIONS NONE;

-- 4: Verify config formatting
(INFO FOR DB).configs;

-- 5: Verify structured config values
(INFO FOR DB STRUCTURE).configs;
