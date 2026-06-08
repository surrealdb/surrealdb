/**
[env]
imports = ["upgrade/define/config_graphql_import.surql"]

[test]
upgrade = true
reason = "GraphQL config (AUTO mode for both tables and functions) survives upgrade"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES AUTO FUNCTIONS AUTO' }"

*/
(INFO FOR DB).configs;
