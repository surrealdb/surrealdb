/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = "Empty-group sentinels for `math::stddev` / `math::variance` under GROUP ALL must be NaN, matching the scalar `math::stddev`/`math::variance` functions. Returning a real number (0.0 — the previous behaviour) silently conflates 'no data' with 'all values identical', which corrupts statistical analysis. The legacy compute-only path returns `[]` for GROUP ALL on empty input by design (no group row produced at all), so this test is scoped to the streaming executor where the empty-group row IS produced and finalized."

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[NaN]"

[[test.results]]
value = "[NaN]"

*/

{
	DEFINE TABLE empty_stats SCHEMALESS;
	RETURN "OK";
};

-- Empty table + GROUP ALL: aggregator finalizes without ever seeing a value.
-- Both functions must return NaN (the same empty sentinel the scalar
-- functions use), not 0.0.
SELECT VALUE math::stddev(value) FROM empty_stats GROUP ALL;
SELECT VALUE math::variance(value) FROM empty_stats GROUP ALL;
