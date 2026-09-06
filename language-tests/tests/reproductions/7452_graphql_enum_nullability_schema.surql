/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
run = false
reason = "Schema fixture for the #7452 GraphQL literal-union nullability reproduction"
*/

DEFINE CONFIG GRAPHQL TABLES AUTO FUNCTIONS NONE;

DEFINE TABLE person SCHEMAFULL;
-- Control: a non-optional scalar is emitted as `String!`.
DEFINE FIELD name ON person TYPE string;
-- Control: an optional scalar is emitted as `String`.
DEFINE FIELD note ON person TYPE option<string>;
-- Subject: a non-optional literal union must be emitted as `<Enum>!`.
DEFINE FIELD status ON person TYPE 'ACTIVE' | 'BANNED';
-- Subject: an optional literal union must stay nullable.
DEFINE FIELD mood ON person TYPE option<'HAPPY' | 'SAD'>;
