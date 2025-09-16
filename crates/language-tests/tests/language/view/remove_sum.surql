
{
	DEFINE TABLE target SCHEMAFULL;
	DEFINE FIELD id ON target TYPE int;
	DEFINE FIELD group ON target TYPE int;
	DEFINE FIELD sum ON target TYPE int;

	INSERT INTO target [
		{
			id: 1,
			group: 1,
			sum: 1,
		},
		{
			id: 2,
			group: 1,
			sum: 1,
		},
		{
			id: 3,
			group: 2,
			sum: 1,
		},
		{
			id: 4,
			group: 2,
			sum: 1,
		}
	];

	DEFINE TABLE view AS
		SELECT
			group,
			math::sum(sum) as groupSum
		FROM target
		GROUP BY group;
};

SELECT * FROM view;
DELETE target:1;
DELETE target:2;
SELECT * FROM view;
