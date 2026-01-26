/**
[test]
reason = "Test ORDER BY with three columns"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ dept: 'Sales', id: item:1, score: 90, team: 'A' }, { dept: 'Sales', id: item:2, score: 85, team: 'A' }, { dept: 'Sales', id: item:3, score: 95, team: 'B' }, { dept: 'Tech', id: item:4, score: 88, team: 'A' }]"

[[test.results]]
value = "[{ dept: 'Tech', id: item:4, score: 88, team: 'A' }, { dept: 'Sales', id: item:2, score: 85, team: 'A' }, { dept: 'Sales', id: item:1, score: 90, team: 'A' }, { dept: 'Sales', id: item:3, score: 95, team: 'B' }]"
*/

-- Setup: Create records with department, team, and score
{
	CREATE item:1 SET dept = "Sales", team = "A", score = 90;
	CREATE item:2 SET dept = "Sales", team = "A", score = 85;
	CREATE item:3 SET dept = "Sales", team = "B", score = 95;
	CREATE item:4 SET dept = "Tech", team = "A", score = 88;
	RETURN "OK";
};

-- Test: Order by dept ASC, team ASC, score DESC
SELECT * FROM item ORDER BY dept ASC, team ASC, score DESC;

-- Test: Order by dept ASC, team ASC, score ASC
SELECT * FROM item ORDER BY dept DESC, team ASC, score ASC;
