/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ before: { mean: 3f }, even: 'DELETE', id: test:triggered_on_view }]"

*/

{
	DEFINE TABLE target;
	DEFINE TABLE view AS SELECT math::mean(field) as mean FROM target GROUP ALL;

	CREATE target:1 SET field = 3;

	DEFINE EVENT event ON view THEN {
		CREATE test:triggered_on_view SET
			input = $input,
			action = $action,
			even = $event,
			before = $before,
			after = $after;
	};
	"OK"
};


SELECT * FROM test;

DELETE target:1;

SELECT * FROM test;
