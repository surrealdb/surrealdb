/**
[test]
reason = "Test ORDER BY with array index access"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, tags: ['alpha', 'zeta'] }, { id: item:2, tags: ['beta', 'epsilon'] }, { id: item:3, tags: ['delta', 'gamma'] }]"

[[test.results]]
value = "[{ id: item:3, tags: ['delta', 'gamma'] }, { id: item:2, tags: ['beta', 'epsilon'] }, { id: item:1, tags: ['alpha', 'zeta'] }]"

[[test.results]]
value = "[{ id: item:2, tags: ['beta', 'epsilon'] }, { id: item:3, tags: ['delta', 'gamma'] }, { id: item:1, tags: ['alpha', 'zeta'] }]"
*/

-- Setup: Create records with arrays
{
	CREATE item:1 SET tags = ["alpha", "zeta"];
	CREATE item:2 SET tags = ["beta", "epsilon"];
	CREATE item:3 SET tags = ["delta", "gamma"];
	RETURN "OK";
};

-- Test ASC: Order by first element in array
SELECT * FROM item ORDER BY tags[0] ASC;

-- Test DESC
SELECT * FROM item ORDER BY tags[0] DESC;

-- Test: Order by second element in array
SELECT * FROM item ORDER BY tags[1] ASC;
