/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
run = false
reason = "Setup for the id_components_* GraphQL tests: a table whose record id names its components, exposed through GraphQL, with three readings"
*/

DEFINE CONFIG GRAPHQL TABLES AUTO FUNCTIONS AUTO;

DEFINE TABLE reading SCHEMAFULL;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE FIELD temp ON reading TYPE int;

CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 4 RETURN NONE;
CREATE reading:['London', d'2024-01-02T00:00:00Z'] SET temp = 5 RETURN NONE;
CREATE reading:['Paris', d'2024-01-01T00:00:00Z'] SET temp = 6 RETURN NONE;
