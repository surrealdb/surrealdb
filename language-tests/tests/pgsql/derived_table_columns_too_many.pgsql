/**
[env]
namespace = true
database = true

[test]
reason = "A column alias list that names more columns than the query has is refused"

[test.results]
request-error = "PostgreSQL SELECT lowering error: planning error: table \"v\" has 1 columns available but 2 columns specified"
*/

SELECT * FROM (SELECT 1 AS k) AS v(a, b);
