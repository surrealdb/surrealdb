/**
[test]
reason = "Test ORDER BY with missing/NONE values in fields"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1 }, { id: item:4, score: NULL }, { id: item:3, score: 50 }, { id: item:2, score: 100 }]"

[[test.results]]
value = "[{ id: item:2, score: 100 }, { id: item:3, score: 50 }, { id: item:4, score: NULL }, { id: item:1 }]"

[[test.results]]
value = "[{ id: item:1 }, { id: item:2, score: 100 }, { id: item:3, score: 50 }, { id: item:4, score: NULL }]"

[[test.results]]
value = "[{ id: item:1 }, { id: item:2, score: 100 }, { id: item:3, score: 50 }, { id: item:4, score: NULL }]"

*/

-- Setup: Create records with some missing fields
{
	CREATE item:1;
	CREATE item:2 SET score = 100;
	CREATE item:3 SET score = 50;
	CREATE item:4 SET score = NULL;
	RETURN "OK";
};

-- Test ASC: Missing values (NONE) come first
SELECT * FROM item ORDER BY score ASC;

-- Test DESC: Missing values come last
SELECT * FROM item ORDER BY score DESC;

-- Test ASC/DESC where all fields are missing. Should return in the sorted key order.
SELECT * FROM item ORDER BY name ASC;
SELECT * FROM item ORDER BY name DESC;
