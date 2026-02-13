/**
[test]
reason = "Test DEFINE CONFIG GRAPHQL with INTROSPECTION setting"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS AUTO INTROSPECTION NONE' }"

[[test.results]]
value = "[{ graphql: { functions: 'AUTO', introspection: NONE, tables: 'AUTO' } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS AUTO' }"

[[test.results]]
value = "[{ graphql: { functions: 'AUTO', tables: 'AUTO' } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS AUTO DEPTH 10 INTROSPECTION NONE' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS AUTO' }"

[env]
namespace = true
database = true
auth = { level = "owner" }
*/

-- 0: Define GraphQL config with INTROSPECTION NONE
DEFINE CONFIG GRAPHQL AUTO INTROSPECTION NONE;

-- 1: Verify the config formatting includes INTROSPECTION NONE
(INFO FOR DB).configs;

-- 2: Verify the structured config values include introspection: NONE
(INFO FOR DB STRUCTURE).configs;

-- 3: Overwrite with INTROSPECTION AUTO (default)
DEFINE CONFIG OVERWRITE GRAPHQL AUTO INTROSPECTION AUTO;

-- 4: Verify the config formatting omits INTROSPECTION (AUTO is default)
(INFO FOR DB).configs;

-- 5: Verify the structured config values omit introspection (AUTO is default)
(INFO FOR DB STRUCTURE).configs;

-- 6: Overwrite with combined DEPTH and INTROSPECTION NONE
DEFINE CONFIG OVERWRITE GRAPHQL AUTO DEPTH 10 INTROSPECTION NONE;

-- 7: Verify the config formatting includes both DEPTH and INTROSPECTION
(INFO FOR DB).configs;

-- 8: Overwrite removing INTROSPECTION (back to default AUTO)
DEFINE CONFIG OVERWRITE GRAPHQL AUTO;

-- 9: Verify INTROSPECTION is omitted
(INFO FOR DB).configs;
