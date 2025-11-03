/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = '[{ id: view_view:[], "math::mean": 1.5f }]'

[[test.results]]
value = "[{ field: 3, g: 2, id: target:2 }]"

[[test.results]]
value = '[{ id: view_view:[], "math::mean": 2f }]'

*/

{
	DEFINE TABLE target;
	DEFINE TABLE view AS SELECT math::mean(field) as mean, g FROM target GROUP g;

	CREATE target:1 SET field = 1,g = 1;
	CREATE target:2 SET field = 2,g = 2;

	DEFINE TABLE view_view AS SELECT math::mean(mean) FROM view GROUP ALL;
	"OK";
};

SELECT * FROM view_view;

UPDATE target:2 SET field = 3;

SELECT * FROM view_view;
