/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
run = false
reason = "Aliased table and fields, with a row, for the nested GRAPHQL_ALIAS reproduction"
*/
DEFINE TABLE widget SCHEMAFULL TYPE NORMAL GRAPHQL_ALIAS 'Widget';
DEFINE FIELD created_at ON widget TYPE datetime GRAPHQL_ALIAS 'createdAt';
DEFINE FIELD price ON widget TYPE object;
DEFINE FIELD price.in_euro ON widget TYPE decimal GRAPHQL_ALIAS 'inEuro';
DEFINE CONFIG GRAPHQL TABLES AUTO INTROSPECTION AUTO;
CREATE widget:1 SET created_at = d'2025-01-01T00:00:00Z', price = { in_euro: 9.5dec };
