/**
[test]
reason = "Test ORDER BY with both COLLATE and NUMERIC modifiers"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: 'File1.txt' }, { id: item:2, value: 'file2.txt' }, { id: item:3, value: 'FILE10.txt' }, { id: item:4, value: 'file20.txt' }]"

[[test.results]]
value = "[{ id: item:4, value: 'file20.txt' }, { id: item:3, value: 'FILE10.txt' }, { id: item:2, value: 'file2.txt' }, { id: item:1, value: 'File1.txt' }]"
*/

-- Setup: Create records with mixed case strings containing numbers
{
	CREATE item:1 SET value = "File1.txt";
	CREATE item:2 SET value = "file2.txt";
	CREATE item:3 SET value = "FILE10.txt";
	CREATE item:4 SET value = "file20.txt";
	RETURN "OK";
};

-- Test with both COLLATE and NUMERIC ASC: case-insensitive natural ordering
SELECT * FROM item ORDER BY value COLLATE NUMERIC ASC;

-- Test with both COLLATE and NUMERIC DESC
SELECT * FROM item ORDER BY value COLLATE NUMERIC DESC;
