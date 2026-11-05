/**
[env]
namespace = true
database = true

[test]
reason = "A domain whose DEFAULT is a constant beyond the range of its integer base type is refused when it is defined"

[test.results]
request-error = "PostgreSQL DDL lowering error: smallint out of range"
*/
CREATE DOMAIN d_small AS smallint DEFAULT 100000;
