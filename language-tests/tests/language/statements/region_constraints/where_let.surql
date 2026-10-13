/**
[env]
namespace = true
database = true

[test]
reason = "Register #28, row ConditionClause: a LET is not allowed in a WHERE clause. The request is refused at plan time, before any statement of it runs."

[test.results]
request-error = "Invalid statement: LET is not allowed in a WHERE clause (statement 2)"
*/
CREATE t:1;
SELECT VALUE id FROM t WHERE { LET $a = 1; $a = 1 };
