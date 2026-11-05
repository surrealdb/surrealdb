/**
[env]
namespace = true
database = true

[test]
reason = "A domain DEFAULT that cannot be read as the domain's base type is refused when the domain is defined, not when the default is first taken"

[test.results]
request-error = "PostgreSQL DDL lowering error: invalid input syntax for type: integer: \"not-an-int\""
*/

CREATE DOMAIN broken AS integer DEFAULT 'not-an-int';
