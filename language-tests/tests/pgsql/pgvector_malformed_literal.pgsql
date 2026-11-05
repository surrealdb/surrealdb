/**
[env]
namespace = true
database = true

[test]
reason = "Text that is not the text form of a vector is refused"

[test.results]
request-error = "PostgreSQL SELECT lowering error: invalid input syntax for type: vector: \"[1,a]\""
*/

SELECT '[1,a]'::vector;
