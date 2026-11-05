/**
[env]
namespace = true
database = true

[test]
reason = "Register #28, row ConditionClause: a SLEEP is not allowed in a WHERE clause. The request is refused at plan time, before any statement of it runs."

[test.results]
request-error = "Invalid statement: SLEEP is not allowed in a WHERE clause (statement 2)"
*/
CREATE t:1;
SELECT VALUE id FROM t WHERE { SLEEP 1ms; true };
