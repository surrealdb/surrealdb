/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL substring FROM a pattern is refused"

[test.results]
request-error = "PostgreSQL SELECT lowering error: unsupported expression: substring with a pattern"
*/

SELECT substring('foobar' FROM 'o.b');
