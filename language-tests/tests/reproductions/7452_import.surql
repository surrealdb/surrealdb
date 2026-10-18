/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
run = false
reason = "Table with a required literal-union field for the GraphQL nullability reproduction"
*/
DEFINE TABLE person SCHEMAFULL TYPE NORMAL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD note ON person TYPE option<string>;
DEFINE FIELD status ON person TYPE 'ACTIVE' | 'BANNED';
DEFINE FIELD maybe ON person TYPE option<'A' | 'B'>;
DEFINE FIELD many ON person TYPE array<'X' | 'Y'>;
DEFINE FIELD nullable ON person TYPE 'P' | 'Q' | null;
DEFINE CONFIG GRAPHQL TABLES AUTO INTROSPECTION AUTO;
