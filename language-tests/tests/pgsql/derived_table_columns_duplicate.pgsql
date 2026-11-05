/**
[env]
namespace = true
database = true

[test]
reason = "A column alias list that uses a name twice is refused"

[test.results]
request-error = "PostgreSQL SELECT lowering error: planning error: column name \"a\" is used by more than one column of table \"v\""
*/

SELECT * FROM (SELECT 1 AS k, 2 AS j) AS v(a, a);
