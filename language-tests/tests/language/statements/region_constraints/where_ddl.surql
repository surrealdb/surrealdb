/**
[env]
namespace = true
database = true

[test]
reason = "Register #28, row ConditionClause: a schema statement is not allowed in a WHERE clause. The request is refused at plan time, before any statement of it runs."

[test.results]
request-error = "Invalid statement: a schema statement is not allowed in a WHERE clause (statement 2)"
*/
CREATE t:1;
SELECT VALUE id FROM t WHERE { DEFINE TABLE zz; true };
