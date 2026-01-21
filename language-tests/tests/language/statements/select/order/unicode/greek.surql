/**
[test]
reason = "Test ORDER BY with Greek alphabet characters"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: 'Α' }, { id: item:2, value: 'Β' }, { id: item:3, value: 'Γ' }, { id: item:4, value: 'Δ' }, { id: item:5, value: 'Ε' }, { id: item:6, value: 'α' }, { id: item:7, value: 'β' }, { id: item:8, value: 'γ' }, { id: item:9, value: 'δ' }, { id: item:10, value: 'ε' }]"

[[test.results]]
value = "[{ id: item:10, value: 'ε' }, { id: item:9, value: 'δ' }, { id: item:8, value: 'γ' }, { id: item:7, value: 'β' }, { id: item:6, value: 'α' }, { id: item:5, value: 'Ε' }, { id: item:4, value: 'Δ' }, { id: item:3, value: 'Γ' }, { id: item:2, value: 'Β' }, { id: item:1, value: 'Α' }]"

[[test.results]]
value = "[{ id: item:1, value: 'Α' }, { id: item:6, value: 'α' }, { id: item:2, value: 'Β' }, { id: item:7, value: 'β' }, { id: item:4, value: 'Δ' }, { id: item:9, value: 'δ' }, { id: item:5, value: 'Ε' }, { id: item:10, value: 'ε' }, { id: item:3, value: 'Γ' }, { id: item:8, value: 'γ' }]"

[[test.results]]
value = "[{ id: item:8, value: 'γ' }, { id: item:3, value: 'Γ' }, { id: item:10, value: 'ε' }, { id: item:5, value: 'Ε' }, { id: item:9, value: 'δ' }, { id: item:4, value: 'Δ' }, { id: item:7, value: 'β' }, { id: item:2, value: 'Β' }, { id: item:6, value: 'α' }, { id: item:1, value: 'Α' }]"
*/

-- Setup: Create records with Greek alphabet characters
{
	CREATE item:1 SET value = "Α";
	CREATE item:2 SET value = "Β";
	CREATE item:3 SET value = "Γ";
	CREATE item:4 SET value = "Δ";
	CREATE item:5 SET value = "Ε";
	CREATE item:6 SET value = "α";
	CREATE item:7 SET value = "β";
	CREATE item:8 SET value = "γ";
	CREATE item:9 SET value = "δ";
	CREATE item:10 SET value = "ε";
	RETURN "OK";
};

-- Test without COLLATE ASC: case-sensitive (uppercase < lowercase)
SELECT * FROM item ORDER BY value ASC;

-- Test without COLLATE DESC
SELECT * FROM item ORDER BY value DESC;

-- Test with COLLATE ASC: case-insensitive lexical ordering
SELECT * FROM item ORDER BY value COLLATE ASC;

-- Test with COLLATE DESC
SELECT * FROM item ORDER BY value COLLATE DESC;
