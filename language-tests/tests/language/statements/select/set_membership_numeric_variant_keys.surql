/**
[test]
reason = "IN / INSIDE / CONTAINS against a numeric literal set agrees with = on which numbers are equal"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[tenth:1, tenth:2]"

[[test.results]]
value = "[tenth:1, tenth:2]"

[[test.results]]
value = "[tenth:1, tenth:2]"

[[test.results]]
value = "[tenth:1, tenth:2]"

[[test.results]]
value = "[tenth:1, tenth:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[tenth:1, tenth:2]"

[[test.results]]
value = "[tenth:1, tenth:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[one:1, one:2, one:3]"

[[test.results]]
value = "[one:1, one:2, one:3]"

[[test.results]]
value = "[one:1, one:2, one:3]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[nest:1, nest:2]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[other:2]"

*/

-- `0.1f` and `0.1dec` are equal to each other but hold different binary values,
-- so a set holding either must match a row holding either.
{
	CREATE tenth:1 SET k = 0.1f, ks = [0.1f];
	CREATE tenth:2 SET k = 0.1dec, ks = [0.1dec];
	RETURN "OK";
};

SELECT VALUE id FROM tenth WHERE k = 0.1dec;
SELECT VALUE id FROM tenth WHERE k IN [0.1dec];
SELECT VALUE id FROM tenth WHERE k IN [0.1f];
SELECT VALUE id FROM tenth WHERE k IN [0.1f, 0.1dec];
SELECT VALUE id FROM tenth WHERE k INSIDE [0.1dec, 99];
SELECT VALUE id FROM tenth WHERE k NOT IN [0.1dec];
SELECT VALUE id FROM tenth WHERE ks CONTAINSANY [0.1dec];
SELECT VALUE id FROM tenth WHERE ks CONTAINSALL [0.1dec];
SELECT VALUE id FROM tenth WHERE ks CONTAINSNONE [0.1dec];

-- Integral values agree across all three numeric variants.
{
	CREATE one:1 SET k = 1;
	CREATE one:2 SET k = 1.0f;
	CREATE one:3 SET k = 1dec;
	RETURN "OK";
};

SELECT VALUE id FROM one WHERE k IN [1];
SELECT VALUE id FROM one WHERE k IN [1.0f];
SELECT VALUE id FROM one WHERE k IN [1dec];

-- A number nested inside a set element, where the element takes the decode path
-- rather than a wire partition.
{
	CREATE nest:1 SET k = { v: 0.1f };
	CREATE nest:2 SET k = { v: 0.1dec };
	RETURN "OK";
};

SELECT VALUE id FROM nest WHERE k IN [{ v: 0.1dec }];

-- Non-matching numbers must still be rejected, and a set with no numbers must
-- not start matching numbers.
{
	CREATE other:1 SET k = 0.2f;
	CREATE other:2 SET k = 7;
	RETURN "OK";
};

SELECT VALUE id FROM other WHERE k IN [0.1dec, 0.3dec];
SELECT VALUE id FROM other WHERE k IN ['0.2', true];
SELECT VALUE id FROM other WHERE k IN [7];
