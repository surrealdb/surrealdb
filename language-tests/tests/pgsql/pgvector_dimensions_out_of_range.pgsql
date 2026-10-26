/**
[env]
namespace = true
database = true

[test]
reason = "A vector has at least one dimension and at most 16000"

[test.results]
request-error = "PostgreSQL DDL lowering error: invalid parameter value: dimensions for type vector cannot exceed 16000"
*/

CREATE TABLE pgv_large (embedding vector(16001));
