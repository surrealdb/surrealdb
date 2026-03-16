/**
[test]
reason = "Test ORDER BY with record ID values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, ref: person:alice }, { id: item:2, ref: person:bob }, { id: item:3, ref: person:charlie }, { id: item:4, ref: person:david }]"

[[test.results]]
value = "[{ id: item:4, ref: person:david }, { id: item:3, ref: person:charlie }, { id: item:2, ref: person:bob }, { id: item:1, ref: person:alice }]"
*/

-- Setup: Create records with record ID values
{
	CREATE item:1 SET ref = person:alice;
	CREATE item:2 SET ref = person:bob;
	CREATE item:3 SET ref = person:charlie;
	CREATE item:4 SET ref = person:david;
	RETURN "OK";
};

-- Test ASC: record IDs ordered by their string representation
SELECT * FROM item ORDER BY ref ASC;

-- Test DESC
SELECT * FROM item ORDER BY ref DESC;
