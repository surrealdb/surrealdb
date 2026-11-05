/**
[test]
reason = """
	`math::stddev` / `math::variance` over no values answer NaN, not 0.0:
	a real number would conflate 'no data' with 'all values identical', which
	corrupts statistical analysis downstream.

	Over an empty table `GROUP ALL` still reports one row, so the sentinel is
	what that row carries, and it is the same value the scalar form gives for
	an empty array — the grand total of no rows is each function applied to no
	values. A single value is the control on the other side: its variance is a
	real 0, because one observation is not 'no data'.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[NaN]"

[[test.results]]
value = "[NaN]"

[[test.results]]
value = "NaN"

[[test.results]]
value = "NaN"

[[test.results]]
value = "[0f]"

[[test.results]]
value = "[0f]"

*/

{
	DEFINE TABLE empty_stats SCHEMALESS;
	RETURN "OK";
};

-- An empty table still has the one GROUP ALL group, so the sentinel is
-- what it finalises to.
SELECT VALUE math::stddev(value) FROM empty_stats GROUP ALL;
SELECT VALUE math::variance(value) FROM empty_stats GROUP ALL;

-- The empty sentinel, on the path that still reaches it.
RETURN math::stddev([]);
RETURN math::variance([]);

-- One observation is not 'no data': its spread is a real zero.
SELECT VALUE math::stddev(value) FROM [{ value: 5 }] GROUP ALL;
SELECT VALUE math::variance(value) FROM [{ value: 5 }] GROUP ALL;
