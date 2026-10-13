/**
[test]
reason = "Numerically equal group keys collapse into one group regardless of which numeric variant carries them"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ count: 5, k: 1, total: 31 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ count: 3, k: 0.1f, total: 7 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ count: 3, k: 1.5f, total: 7 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ count: 1, k: 1, total: 4 }, { count: 2, k: 1.5f, total: 10 }, { count: 2, k: 2dec, total: 17 }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ a: 1, b: 2dec, count: 2, total: 3 }, { a: 1f, b: 2.5f, count: 1, total: 4 }]"

*/

-- `1`, `1.0f` and three decimals that differ only in scale are all equal to each
-- other, so every row belongs to the same group.
{
	CREATE scale:1 SET k = 1, v = 1;
	CREATE scale:2 SET k = 1.0f, v = 2;
	CREATE scale:3 SET k = 1dec, v = 4;
	CREATE scale:4 SET k = 1.0dec, v = 8;
	CREATE scale:5 SET k = 1.00dec, v = 16;
	RETURN "OK";
};

SELECT k, count() AS count, math::sum(v) AS total FROM scale GROUP BY k;

-- A float and a decimal that print the same but hold different binary values.
{
	CREATE tenth:1 SET k = 0.1f, v = 1;
	CREATE tenth:2 SET k = 0.1dec, v = 2;
	CREATE tenth:3 SET k = 0.10dec, v = 4;
	RETURN "OK";
};

SELECT k, count() AS count, math::sum(v) AS total FROM tenth GROUP BY k;

-- Trailing scale on a non-integral decimal.
{
	CREATE half:1 SET k = 1.5f, v = 1;
	CREATE half:2 SET k = 1.5dec, v = 2;
	CREATE half:3 SET k = 1.50dec, v = 4;
	RETURN "OK";
};

SELECT k, count() AS count, math::sum(v) AS total FROM half GROUP BY k;

-- Distinct numeric keys must still separate, and the group order is the key
-- order.
{
	CREATE spread:1 SET k = 2dec, v = 1;
	CREATE spread:2 SET k = 1.5f, v = 2;
	CREATE spread:3 SET k = 1, v = 4;
	CREATE spread:4 SET k = 1.50dec, v = 8;
	CREATE spread:5 SET k = 2.0f, v = 16;
	RETURN "OK";
};

SELECT k, count() AS count, math::sum(v) AS total FROM spread GROUP BY k;

-- A two-column key: the same collapsing applies per column.
{
	CREATE pair:1 SET a = 1, b = 2.0dec, v = 1;
	CREATE pair:2 SET a = 1.00dec, b = 2, v = 2;
	CREATE pair:3 SET a = 1.0f, b = 2.5f, v = 4;
	RETURN "OK";
};

SELECT a, b, count() AS count, math::sum(v) AS total FROM pair GROUP BY a, b;
