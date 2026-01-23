/**
[test]
reason = "Test ORDER BY with Greek alphabet characters"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ value: 'Α' }, { value: 'Β' }, { value: 'Γ' }, { value: 'Δ' }, { value: 'Ε' }, { value: 'α' }, { value: 'β' }, { value: 'γ' }, { value: 'δ' }, { value: 'ε' }]"

[[test.results]]
value = "[{ value: 'ε' }, { value: 'δ' }, { value: 'γ' }, { value: 'β' }, { value: 'α' }, { value: 'Ε' }, { value: 'Δ' }, { value: 'Γ' }, { value: 'Β' }, { value: 'Α' }]"

[[test.results]]
value = "[{ value: 'Α' }, { value: 'α' }, { value: 'Β' }, { value: 'β' }, { value: 'Δ' }, { value: 'δ' }, { value: 'Ε' }, { value: 'ε' }, { value: 'Γ' }, { value: 'γ' }]"

[[test.results]]
value = "[{ value: 'γ' }, { value: 'Γ' }, { value: 'ε' }, { value: 'Ε' }, { value: 'δ' }, { value: 'Δ' }, { value: 'β' }, { value: 'Β' }, { value: 'α' }, { value: 'Α' }]"

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
SELECT `value` FROM item ORDER BY value ASC;

-- Test without COLLATE DESC
SELECT `value` FROM item ORDER BY value DESC;

-- Test with COLLATE ASC: case-insensitive lexical ordering
SELECT `value` FROM item ORDER BY value COLLATE ASC;

-- Test with COLLATE DESC
SELECT `value` FROM item ORDER BY value COLLATE DESC;
