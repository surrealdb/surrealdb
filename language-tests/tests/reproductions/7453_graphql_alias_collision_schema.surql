/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
run = false
reason = "Schema fixture for the #7453 alias collision reproduction: the alias names the table's Object type, so it can now collide with a built-in one"
*/

DEFINE CONFIG GRAPHQL TABLES AUTO FUNCTIONS NONE;

DEFINE TABLE widget SCHEMAFULL GRAPHQL_ALIAS 'PageInfo';
DEFINE FIELD label ON widget TYPE string;
