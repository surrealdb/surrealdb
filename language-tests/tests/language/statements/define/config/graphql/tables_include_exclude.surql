/**
[test]
reason = "Test DEFINE CONFIG GRAPHQL TABLES INCLUDE and TABLES EXCLUDE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES INCLUDE foo, bar FUNCTIONS NONE' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES EXCLUDE internal FUNCTIONS NONE' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS NONE' }"

[[test.results]]
value = "[{ graphql: { functions: NONE, tables: 'AUTO' } }]"

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

-- 0-2: Define some tables to reference in INCLUDE/EXCLUDE
DEFINE TABLE foo SCHEMAFUL;
DEFINE TABLE bar SCHEMAFUL;
DEFINE TABLE internal SCHEMAFUL;

-- 3: Define GraphQL config with TABLES INCLUDE (only specified tables exposed)
DEFINE CONFIG GRAPHQL TABLES INCLUDE foo, bar;

-- 4: Verify config formatting
(INFO FOR DB).configs;

-- 5: Overwrite with TABLES EXCLUDE (all tables except specified)
DEFINE CONFIG OVERWRITE GRAPHQL TABLES EXCLUDE internal;

-- 6: Verify config formatting
(INFO FOR DB).configs;

-- 7: Overwrite with TABLES AUTO
DEFINE CONFIG OVERWRITE GRAPHQL TABLES AUTO;

-- 8: Verify config formatting and structured values
(INFO FOR DB).configs;
(INFO FOR DB STRUCTURE).configs;

-- 10: Overwrite with TABLES NONE
DEFINE CONFIG OVERWRITE GRAPHQL TABLES NONE;

-- 11: Verify config formatting and structured values
(INFO FOR DB).configs;
(INFO FOR DB STRUCTURE).configs;
