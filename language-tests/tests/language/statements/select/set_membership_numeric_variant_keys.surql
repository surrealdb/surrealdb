/**
[test]
reason = "IN / INSIDE / CONTAINS against a numeric literal set agrees with = on which numbers are equal: one value in several kinds matches as one, and a float and a decimal that are not exactly equal match apart"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[half:1, half:2]"

[[test.results]]
value = "[half:1, half:2]"

[[test.results]]
value = "[half:1, half:2]"

[[test.results]]
value = "[half:1, half:2]"

[[test.results]]
value = "[half:1, half:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[half:1, half:2]"

[[test.results]]
value = "[half:1, half:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[half:1, half:2]"

[[test.results]]
value = "[half:1, half:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[half:1, half:2]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[tenth:2]"

[[test.results]]
value = "[tenth:1]"

[[test.results]]
value = "[tenth:1]"

[[test.results]]
value = "[tenth:2]"

[[test.results]]
value = "[tenth:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[tenth:1, tenth:2]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[pair:2]"

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
value = "[]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[other:2]"
*/

-- `0.5f` and `0.5dec` are one value held in two kinds, so a set holding either
-- must match a row holding either.
{
	CREATE half:1 SET k = 0.5f, ks = [0.5f];
	CREATE half:2 SET k = 0.5dec, ks = [0.5dec];
	RETURN "OK";
};

SELECT VALUE id FROM half WHERE k = 0.5dec;
SELECT VALUE id FROM half WHERE k IN [0.5dec];
SELECT VALUE id FROM half WHERE k IN [0.5f];
SELECT VALUE id FROM half WHERE k IN [0.5f, 0.5dec];
SELECT VALUE id FROM half WHERE k INSIDE [0.5dec, 99];
SELECT VALUE id FROM half WHERE k NOT IN [0.5dec];
SELECT VALUE id FROM half WHERE ks CONTAINSANY [0.5dec];
SELECT VALUE id FROM half WHERE ks CONTAINSALL [0.5dec];
SELECT VALUE id FROM half WHERE ks CONTAINSNONE [0.5dec];

-- A literal list that spells one value two ways asks for that value once, so a
-- row holding either spelling satisfies CONTAINSALL.
SELECT VALUE id FROM half WHERE ks CONTAINSALL [0.5f, 0.5dec];
SELECT VALUE id FROM half WHERE ks CONTAINSANY [0.5f, 0.5dec];
SELECT VALUE id FROM half WHERE ks CONTAINSNONE [0.5f, 0.5dec];
SELECT VALUE id FROM half WHERE k IN [0.5f, 0.5dec];

-- `0.1f` and `0.1dec` are two values: a float equals a decimal only where the
-- decimal spells the float's exact value, and `0.1f` is
-- `0.100000000000000005551115123125782702118`.
{
	CREATE tenth:1 SET k = 0.1f, ks = [0.1f];
	CREATE tenth:2 SET k = 0.1dec, ks = [0.1dec];
	RETURN "OK";
};

SELECT VALUE id FROM tenth WHERE k = 0.1dec;
SELECT VALUE id FROM tenth WHERE k IN [0.1f];
SELECT VALUE id FROM tenth WHERE k NOT IN [0.1dec];
SELECT VALUE id FROM tenth WHERE ks CONTAINSANY [0.1dec];
SELECT VALUE id FROM tenth WHERE ks CONTAINSNONE [0.1dec];
SELECT VALUE id FROM tenth WHERE ks CONTAINSALL [0.1f, 0.1dec];
SELECT VALUE id FROM tenth WHERE ks CONTAINSANY [0.1f, 0.1dec];

-- Two genuinely distinct literals still need both present.
{
	CREATE pair:1 SET ks = [0.5f];
	CREATE pair:2 SET ks = [0.5dec, 0.25f];
	RETURN "OK";
};

SELECT VALUE id FROM pair WHERE ks CONTAINSALL [0.5dec, 0.25dec];

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
	CREATE nest:1 SET k = { v: 0.5f };
	CREATE nest:2 SET k = { v: 0.5dec };
	CREATE nest:3 SET k = { v: 0.1f };
	RETURN "OK";
};

SELECT VALUE id FROM nest WHERE k IN [{ v: 0.5dec }];
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
