/**
[test]
run = false
reason = "Setup only: a schemafull table with typed, optional and nested fields and a schemaless table, for the tests that read them through the PostgreSQL system views."
*/

DEFINE TABLE catalog_user SCHEMAFULL;
DEFINE FIELD id ON catalog_user TYPE int;
DEFINE FIELD name ON catalog_user TYPE string;
DEFINE FIELD tags ON catalog_user TYPE option<array<string>>;
DEFINE FIELD address ON catalog_user TYPE object;
DEFINE FIELD address.city ON catalog_user TYPE string;

DEFINE TABLE catalog_note SCHEMALESS;
