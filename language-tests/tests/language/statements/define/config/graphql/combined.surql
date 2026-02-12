/**
[test]
reason = "Test DEFINE CONFIG GRAPHQL with combined TABLES and FUNCTIONS settings"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES INCLUDE users, posts FUNCTIONS AUTO' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS NONE' }"

[[test.results]]
value = "[{ graphql: { functions: NONE, tables: 'AUTO' } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES EXCLUDE users FUNCTIONS AUTO' }"

[env]
namespace = true
database = true
auth = { level = "owner" }
*/

-- 0-1: Define some tables
DEFINE TABLE users SCHEMAFUL;
DEFINE TABLE posts SCHEMAFUL;

-- 2: Define GraphQL config with TABLES INCLUDE and FUNCTIONS AUTO
DEFINE CONFIG GRAPHQL TABLES INCLUDE users, posts FUNCTIONS AUTO;

-- 3: Verify config formatting
(INFO FOR DB).configs;

-- 4: Overwrite with TABLES AUTO and FUNCTIONS NONE
DEFINE CONFIG OVERWRITE GRAPHQL TABLES AUTO FUNCTIONS NONE;

-- 5: Verify config formatting and structured values
(INFO FOR DB).configs;
(INFO FOR DB STRUCTURE).configs;

-- 7: Overwrite with TABLES EXCLUDE and FUNCTIONS AUTO
DEFINE CONFIG OVERWRITE GRAPHQL TABLES EXCLUDE users FUNCTIONS AUTO;

-- 8: Verify config formatting
(INFO FOR DB).configs;
