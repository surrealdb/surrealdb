/**
[env]
namespace = true
database = true

[test]
reason = "A vector column declares its dimensions"

[test.results]
request-error = "PostgreSQL DDL lowering error: unsupported statement: a vector column without dimensions; declare it as vector(n)"
*/

CREATE TABLE pgv_bare (embedding vector);
