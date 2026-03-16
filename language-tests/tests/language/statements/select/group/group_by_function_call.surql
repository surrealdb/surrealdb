/**
[test]
reason = "Test GROUP BY with datetime and duration fields"
wip = true

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ day: 15, count: 2 }, { day: 16, count: 2 }, { day: 17, count: 1 }]"

*/

-- Setup data
{
	CREATE event:1 SET timestamp = d'2024-01-15T10:00:00Z', duration = 1h, value = 10;
	CREATE event:2 SET timestamp = d'2024-01-15T14:00:00Z', duration = 2h, value = 20;
	CREATE event:3 SET timestamp = d'2024-01-16T10:00:00Z', duration = 1h, value = 15;
	CREATE event:4 SET timestamp = d'2024-01-16T15:00:00Z', duration = 2h, value = 25;
	CREATE event:5 SET timestamp = d'2024-01-17T09:00:00Z', duration = 3h, value = 30;
	RETURN "OK";
};

-- Test grouping by date function (currently not supported - functions not allowed in GROUP BY)
SELECT time::day(timestamp) as day, COUNT() as count FROM event GROUP BY time::day(timestamp);
