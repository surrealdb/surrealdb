/**
[test]
reason = "Test ALTER CONFIG with all config types"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS AUTO' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS NONE' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ API: 'API PERMISSIONS FULL', GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS NONE' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ API: 'API PERMISSIONS NONE', GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS NONE' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ API: 'API PERMISSIONS NONE', Default: 'DEFAULT NAMESPACE test DATABASE testdb', GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS NONE' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ API: 'API PERMISSIONS NONE', Default: 'DEFAULT NAMESPACE updated DATABASE testdb', GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS NONE' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The config for default does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

-- GRAPHQL: Define initial config
DEFINE CONFIG GRAPHQL AUTO;
(INFO FOR DB).configs;

-- GRAPHQL: Alter tables and functions
ALTER CONFIG GRAPHQL TABLES AUTO FUNCTIONS NONE;
(INFO FOR DB).configs;

-- API: Define initial config
DEFINE CONFIG API PERMISSIONS FULL;
(INFO FOR DB).configs;

-- API: Alter permissions
ALTER CONFIG API PERMISSIONS NONE;
(INFO FOR DB).configs;

-- DEFAULT: Alter creates config at DB level (upsert behavior)
ALTER CONFIG DEFAULT NAMESPACE test DATABASE testdb;
(INFO FOR DB).configs;

-- DEFAULT: Alter existing config with updated values
ALTER CONFIG DEFAULT NAMESPACE updated DATABASE testdb;
(INFO FOR DB).configs;

-- Clean up configs
REMOVE CONFIG GRAPHQL;
REMOVE CONFIG API;
-- REMOVE CONFIG DEFAULT errors because ALTER stores at DB level but REMOVE checks Root level
REMOVE CONFIG DEFAULT;

-- IF EXISTS: returns NONE on non-existent config
ALTER CONFIG IF EXISTS GRAPHQL AUTO;
ALTER CONFIG IF EXISTS API PERMISSIONS FULL;
ALTER CONFIG IF EXISTS DEFAULT NAMESPACE test;
