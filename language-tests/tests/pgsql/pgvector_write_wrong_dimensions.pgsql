/**
[env]
namespace = true
database = true
imports = ["pgsql/_pgvector_native.surql"]

[test]
reason = "A text literal written to a vector column is read as a vector of the dimensions the column is declared with, and is refused when it has others, as pgvector refuses it"

[test.results]
request-error = "PostgreSQL INSERT lowering error: data exception: expected 3 dimensions, not 2"
*/

INSERT INTO pgv_native (id, embedding) VALUES (1, '[1,2]');
