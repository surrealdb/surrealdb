/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Duplicate computed aliases must not synthesize a name that shadows a selected source field"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ bar: 2, foo: 99 }]"
*/
{
	CREATE t:1 SET _e0 = 99;
	RETURN "OK";
};

SELECT _e0 AS foo, 1 AS bar, 2 AS bar FROM t;
