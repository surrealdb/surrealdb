/**
[env]
namespace = true
database = true

[test]
reason = "A cast to vector(n) of text that has another number of elements is a data exception"

[test.results]
request-error = "PostgreSQL SELECT lowering error: data exception: expected 3 dimensions, not 2"
*/

SELECT '[1,2]'::vector(3);
