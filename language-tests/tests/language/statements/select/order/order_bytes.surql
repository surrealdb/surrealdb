/**
[test]
reason = "Test ORDER BY with bytes values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '[{ id: item:1, value: b"00" }, { id: item:4, value: b"ABAB" }, { id: item:2, value: b"CAFEBEEF" }, { id: item:3, value: b"CAFEBEEF" }]'

[[test.results]]
value = '[{ id: item:2, value: b"CAFEBEEF" }, { id: item:3, value: b"CAFEBEEF" }, { id: item:4, value: b"ABAB" }, { id: item:1, value: b"00" }]'

*/

-- Setup: Create records with bytes values
{
	CREATE item:1 SET value = b"00";
	CREATE item:2 SET value = b"CAFEBEEF";
	CREATE item:3 SET value = b"cafebeef";
	CREATE item:4 SET value = b"\u{61}\u{62}\u0061\u0062";
	RETURN "OK";
};

-- Test ASC: byte-by-byte comparison
SELECT * FROM item ORDER BY value ASC;

-- Test DESC
SELECT * FROM item ORDER BY value DESC;
