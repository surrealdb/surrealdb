/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = """
	Plan companion to group_by_parent_param_source: the refusal of a
	`$parent` read inside a grouped projection is a plan-time rule, so EXPLAIN
	raises the GROUP BY row-scope error for a nested SELECT over `$parent`
	beside GROUP BY and beside GROUP ALL, and prints no plan.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
error = """
Invalid query: Found a `$parent` parameter refering to the document of a GROUP select statement
Select statements with a GROUP BY or GROUP ALL currently have no defined document to refer to"""

[[test.results]]
error = """
Invalid query: Found a `$parent` parameter refering to the document of a GROUP select statement
Select statements with a GROUP BY or GROUP ALL currently have no defined document to refer to"""

[[test.results]]
error = """
Invalid query: Found a `$parent` parameter refering to the document of a GROUP select statement
Select statements with a GROUP BY or GROUP ALL currently have no defined document to refer to"""
*/

{
	CREATE a:1 SET v = 1, g = 1;
	CREATE a:2 SET v = 2, g = 1;
	CREATE a:3 SET v = 3, g = 2;
	CREATE a:4 SET v = 4, g = 2;
	RETURN "OK";
};

EXPLAIN SELECT (SELECT id FROM $parent) AS p, g, math::mean(v) FROM a GROUP BY g;

EXPLAIN SELECT (SELECT count() FROM $parent GROUP ALL) AS c, g FROM a GROUP BY g;

EXPLAIN SELECT (SELECT id FROM $parent) AS p, count() FROM a GROUP ALL;
