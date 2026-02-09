/**
[test]
reason = "Test ORDER BY with NUMERIC for natural number sorting in strings"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: 'file1.txt' }, { id: item:2, value: 'file10.txt' }, { id: item:3, value: 'file2.txt' }, { id: item:4, value: 'file20.txt' }]"

[[test.results]]
value = "[{ id: item:4, value: 'file20.txt' }, { id: item:3, value: 'file2.txt' }, { id: item:2, value: 'file10.txt' }, { id: item:1, value: 'file1.txt' }]"

[[test.results]]
value = "[{ id: item:1, value: 'file1.txt' }, { id: item:3, value: 'file2.txt' }, { id: item:2, value: 'file10.txt' }, { id: item:4, value: 'file20.txt' }]"

[[test.results]]
value = "[{ id: item:4, value: 'file20.txt' }, { id: item:2, value: 'file10.txt' }, { id: item:3, value: 'file2.txt' }, { id: item:1, value: 'file1.txt' }]"
*/

-- Setup: Create records with strings containing numbers
{
	CREATE item:1 SET value = "file1.txt";
	CREATE item:2 SET value = "file10.txt";
	CREATE item:3 SET value = "file2.txt";
	CREATE item:4 SET value = "file20.txt";
	RETURN "OK";
};

-- Test without NUMERIC: lexical ordering ("10" < "2")
SELECT * FROM item ORDER BY value ASC;

-- Test without NUMERIC DESC
SELECT * FROM item ORDER BY value DESC;

-- Test with NUMERIC ASC: natural ordering (1 < 2 < 10 < 20)
SELECT * FROM item ORDER BY value NUMERIC ASC;

-- Test with NUMERIC DESC
SELECT * FROM item ORDER BY value NUMERIC DESC;
