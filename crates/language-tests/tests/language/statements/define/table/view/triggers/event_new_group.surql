/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ field: 3, id: target:1 }]"

[[test.results]]
value = "[{ after: { mean: 3f }, even: 'CREATE', id: test:triggered_on_view }]"

*/

{
	DEFINE TABLE target;
	DEFINE TABLE view AS SELECT math::mean(field) as mean FROM target GROUP ALL;


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

CREATE target:1 SET field = 3;

SELECT * FROM test;
