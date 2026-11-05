/**
[env]
namespace = true
database = true
imports = ["pgsql/_pgvector_native.surql"]

[test]
reason = "The distance operators are not supported in the WHERE clause of a write"

[test.results]
request-error = "PostgreSQL DELETE lowering error: unsupported expression: the <-> operator in a write"
*/

DELETE FROM pgv_native WHERE embedding <-> '[1,2,3]' < 1;
