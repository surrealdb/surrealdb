/**
[test]
reason = "Test GROUP ALL with WHERE clause for filtering before aggregating entire dataset"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ avg_temp: 22.333333333333332f, count: 3 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 2, total: 47f }]"

*/

-- Setup data
{
	CREATE sensor:1 SET temperature = 20, active = true;
	CREATE sensor:2 SET temperature = 25, active = true;
	CREATE sensor:3 SET temperature = 30, active = false;
	CREATE sensor:4 SET temperature = 22, active = true;
	CREATE sensor:5 SET temperature = 28, active = false;
	RETURN "OK";
};

-- Test WHERE with GROUP ALL
SELECT math::mean(temperature) as avg_temp, COUNT() as count FROM sensor WHERE active = true GROUP ALL;

-- Test WHERE filtering all records
SELECT COUNT() as count FROM sensor WHERE temperature > 100 GROUP ALL;

-- Test WHERE with multiple conditions
SELECT math::sum(temperature) as total, COUNT() as count FROM sensor WHERE active = true AND temperature > 20 GROUP ALL;
