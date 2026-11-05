/**
[test]
reason = "Register #28, row ConditionClause, admitted control: a schema statement is admitted in a field list. Statements one, two and five of language/subquery/schema_in_expression_seen_by_later_expressions.surql, with their results."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"

[[test.results]]
value = "[{ a: 2, c: 2, id: t:1, w: NONE }, { a: 2, c: 2, id: t:2, w: NONE }]"
*/
DEFINE FUNCTION fn::f() { RETURN 1 };
CREATE t:1, t:2;
SELECT id, (DEFINE FUNCTION OVERWRITE fn::f() { RETURN 2 }) AS w, fn::f() AS a, (RETURN fn::f()) AS c FROM t;
