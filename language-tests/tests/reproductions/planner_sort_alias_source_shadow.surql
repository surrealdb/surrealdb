/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "ORDER BY alias Compute fields must not shadow selected source fields before projection"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ _e0: 1, foo: 99 }]"
*/
{
	CREATE t:1 SET _e0 = 99;
	RETURN "OK";
};

SELECT _e0 AS foo, 1 AS _e0 FROM t ORDER BY _e0;
