/**
[env]
namespace = true
database = true

[test]
reason = "A domain whose DEFAULT is a boolean constant over an integer base type is refused when it is defined"

[test.results]
request-error = "PostgreSQL DDL lowering error: column \"d\" is of type integer but default expression is of type boolean"
*/
CREATE DOMAIN d AS integer DEFAULT true;
