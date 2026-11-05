/**
[env]
namespace = true
database = true
imports = ["pgsql/_pgvector_native.surql"]

[test]
reason = "A text literal written to a vector column that is not the text form of a vector is refused"

[test.results]
request-error = "PostgreSQL INSERT lowering error: invalid input syntax for type: vector: \"[1,x,3]\""
*/

INSERT INTO pgv_native (id, embedding) VALUES (1, '[1,x,3]');
