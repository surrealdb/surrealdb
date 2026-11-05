/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
run = false
reason = "Setup for the id_components_create_default GraphQL test: a table whose record id names its components and has a DEFAULT, exposed through GraphQL"
*/

DEFINE CONFIG GRAPHQL TABLES AUTO FUNCTIONS AUTO;

DEFINE TABLE sensor SCHEMAFULL;
DEFINE FIELD id ON sensor TYPE [city: string, time: datetime] DEFAULT ['Rome', d'2024-02-01T00:00:00Z'];
DEFINE FIELD temp ON sensor TYPE int;
