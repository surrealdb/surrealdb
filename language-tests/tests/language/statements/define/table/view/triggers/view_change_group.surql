/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = '[{ id: view_view:[], "math::sum": 3f }]'

[[test.results]]
value = "[{ field: 2, g: 1, id: target:2 }]"

[[test.results]]
value = '[{ id: view_view:[], "math::sum": 1.5f }]'

*/

{
	DEFINE TABLE target;
	DEFINE TABLE view AS SELECT math::mean(field) as mean, g FROM target GROUP g;

	CREATE target:1 SET field = 1,g = 1;
	CREATE target:2 SET field = 2,g = 2;

	DEFINE TABLE view_view AS SELECT math::sum(mean) FROM view GROUP ALL;
	"OK";
};

SELECT * FROM view_view;

UPDATE target:2 SET g = 1;

SELECT * FROM view_view;
