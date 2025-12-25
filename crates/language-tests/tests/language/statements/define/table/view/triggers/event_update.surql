/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
error = "The table 'test' does not exist"

[[test.results]]
value = "[{ field: 3, id: target:2 }]"

[[test.results]]
value = "[{ after: { mean: 2f }, before: { mean: 1.5f }, even: 'UPDATE', id: test:triggered_on_view }]"

*/

{
	DEFINE TABLE target;
	DEFINE TABLE view AS SELECT math::mean(field) as mean FROM target GROUP ALL;

	CREATE target:1 SET field = 1;
	CREATE target:2 SET field = 2;

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

UPDATE target:2 SET field = 3;

SELECT * FROM test;

