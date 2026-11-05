/**
[env]
namespace = true
database = true

[test]
reason = "A domain whose DEFAULT is a numeric constant over a boolean base type is refused when it is defined"

[test.results]
request-error = "PostgreSQL DDL lowering error: column \"d\" is of type boolean but default expression is of type integer"
*/
CREATE DOMAIN d AS boolean DEFAULT 1;
