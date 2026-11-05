/**
[test]
reason = "Register #28, row ConditionClause, admitted control: the construct the WHERE clause refuses is admitted in a field list. Statements two and ten of language/subquery/schema_in_expression_seen_by_later_expressions.surql, with their results."

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"

[[test.results]]
value = "[{ id: t:1, r: NONE, v: 42 }, { id: t:2, r: NONE, v: 42 }]"
*/
CREATE t:1, t:2;
SELECT id, (DEFINE FIELD OVERWRITE f ON ua VALUE 42) AS r, (CREATE ONLY ua RETURN VALUE f) AS v FROM t;
