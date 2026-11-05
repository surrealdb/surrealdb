/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
run = false
reason = "Schemaless table under auto GraphQL for the empty update input reproduction"
*/
DEFINE CONFIG GRAPHQL AUTO;
DEFINE TABLE thing SCHEMALESS;
CREATE thing:1;
