/**
[env]
namespace = true
database = true

[test]
reason = "A view over the * of a relation is refused: the definition of a view does not expand it"

[test.results]
request-error = "PostgreSQL DDL lowering error: unsupported expression: t.* is supported only in the select list of a SELECT statement"
*/

CREATE VIEW qw_view AS SELECT t.* FROM qw_src t;
