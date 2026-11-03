/**
[env]
namespace = true
database = true

[test]
reason = "A domain whose DEFAULT is a constant with more integer digits than its numeric base type's precision less its scale is refused when it is defined"

[test.results]
request-error = "PostgreSQL DDL lowering error: numeric field overflow"
*/
CREATE DOMAIN d_num AS numeric(3,1) DEFAULT -100;
