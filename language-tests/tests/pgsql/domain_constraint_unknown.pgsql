/**
[env]
namespace = true
database = true

[test]
reason = "ALTER DOMAIN ... DROP CONSTRAINT of a constraint the domain does not have is refused when the request is planned"

[test.results]
request-error = "PostgreSQL DDL lowering error: constraint \"nope\" of domain \"posint\" does not exist"
*/

CREATE DOMAIN posint AS integer CHECK (VALUE > 0);

ALTER DOMAIN posint DROP CONSTRAINT nope;
