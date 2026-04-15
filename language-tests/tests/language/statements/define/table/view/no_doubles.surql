/**
[test]
issue = 3556

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ activeRounds: 3, id: monthly:[2024, 1], month: 1, year: 2024 }]"

[[test.results]]
value = "[{ activeRounds: 3, day: 1, id: daily:[2024, 1, 1], month: 1, year: 2024 }]"

*/
{
	CREATE happy:1 SET year=2024, month=1, day=1;
	CREATE happy:2 SET year=2024, month=1, day=1;
	CREATE happy:3 SET year=2024, month=1, day=1;
	DEFINE TABLE monthly AS SELECT count() as activeRounds, year, month FROM happy GROUP BY year, month;
	DEFINE TABLE daily AS SELECT count() as activeRounds, year, month, day FROM happy GROUP BY year, month, day;
	RETURN "OK";
};

SELECT * FROM monthly;
SELECT * FROM daily;
