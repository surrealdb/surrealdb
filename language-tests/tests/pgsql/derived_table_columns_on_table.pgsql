/**
[env]
namespace = true
database = true

[test]
reason = "A column alias list on a table is refused: the table's columns have no declared order"

[test.results]
request-error = "PostgreSQL SELECT lowering error: unsupported statement: a column alias list on a table: the table's columns have no declared order"
*/

SELECT * FROM t AS v(a);
