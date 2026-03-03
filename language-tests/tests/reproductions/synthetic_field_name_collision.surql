/**
[test]
reason = "Ensure synthetic _eN names from the expression registry do not collide with user-defined field names"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ _e0: 'original', id: t:1, val: 10 }, { _e0: 'second', id: t:2, val: 20 }]"

[[test.results]]
value = "[{ _e0: 'original', doubled: 20, id: t:1, val: 10 }, { _e0: 'second', doubled: 40, id: t:2, val: 20 }]"

[[test.results]]
value = "[{ _e0: 'second', doubled: 40, id: t:2, val: 20 }, { _e0: 'original', doubled: 20, id: t:1, val: 10 }]"

*/

-- Setup: Create records with a field named _e0 (matches the synthetic name pattern)
{
	CREATE t:1 SET _e0 = "original", val = 10;
	CREATE t:2 SET _e0 = "second", val = 20;
	RETURN "OK";
};

-- Verify _e0 field exists in the data
SELECT * FROM t ORDER BY id;

-- Test: SELECT with computed alias + ORDER BY alias (ASC)
-- The expression registry must not overwrite the user's _e0 field
SELECT *, val * 2 AS doubled FROM t ORDER BY doubled ASC;

-- Same query but DESC to verify ordering works correctly
SELECT *, val * 2 AS doubled FROM t ORDER BY doubled DESC;
