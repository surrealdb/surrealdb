/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = '[{ id: view_view:[], "math::mean": 2f }]'

[[test.results]]
value = "[]"

[[test.results]]
value = '[{ id: view_view:[], "math::mean": 1.5f }]'

*/

{
	DEFINE TABLE target;
	DEFINE TABLE view AS SELECT math::mean(field) as mean, g FROM target GROUP g;

	CREATE target:1 SET field = 1,g = 1;
	CREATE target:2 SET field = 2,g = 2;
	CREATE target:3 SET field = 3,g = 3;

	DEFINE TABLE view_view AS SELECT math::mean(mean) FROM view GROUP ALL;
	"OK";
};

SELECT * FROM view_view;

DELETE target:3;

SELECT * FROM view_view;
