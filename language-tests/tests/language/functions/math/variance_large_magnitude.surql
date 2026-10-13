/**
[env]
namespace = true
database = true

[test]
issue = 477
reason = "Integer differences widen to f64 before squaring, since Int*Int is a checked i64 multiply that overflows past sqrt(i64::MAX). math::variance and math::stddev accumulate sum and sum-of-squares of `x - shift` rather than of `x`. Finalising `(sum_of_squares - sum * mean) / (n - 1)` on raw values subtracts two O(n*x^2) terms that agree to more digits than the accumulator carries: over [1e8+1, 1e8+2, 1e8+4] the true variance 2.333 came back as 2. Shifting by the group's first value keeps both terms small. The state stays invertible, so materialized-view maintenance stays incremental -- exercised here by a backfilled view, a view built incrementally, and a delete and an update against it. Decimal groups accumulate in Decimal and match the scalar exactly."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 100000001f, g: 'a', id: u:1 }]"

[[test.results]]
value = "[{ f: 100000002f, g: 'a', id: u:2 }]"

[[test.results]]
value = "[{ f: 100000004f, g: 'a', id: u:3 }]"

[[test.results]]
value = "[{ g: 'a', s: 1.5275252316519468f, v: 2.3333333333333335f }]"

[[test.results]]
value = "2.333333333333333f"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[2.3333333333333335f]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ d: 1.5dec, g: 'a', id: t:1 }]"

[[test.results]]
value = "[{ d: 2.5dec, g: 'a', id: t:2 }]"

[[test.results]]
value = "[{ d: 4.5dec, g: 'a', id: t:3 }]"

[[test.results]]
value = "[{ g: 'a', v: 2.3333333333333333333333333334dec }]"

[[test.results]]
value = "2.3333333333333333333333333334dec"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 100000001f, g: 'a', id: w:1 }]"

[[test.results]]
value = "[{ f: 100000002f, g: 'a', id: w:2 }]"

[[test.results]]
value = "[{ f: 100000004f, g: 'a', id: w:3 }]"

[[test.results]]
value = "[2.3333333333333335f]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[0.5f]"

[[test.results]]
value = "0.5f"

[[test.results]]
value = "[{ f: 100000009f, g: 'a', id: w:2 }]"

[[test.results]]
value = "[32f]"

[[test.results]]
value = "32f"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ g: 'a', id: i:1, n: 0 }]"

[[test.results]]
value = "[{ g: 'a', id: i:2, n: 4000000000 }]"

[[test.results]]
value = "[{ g: 'a', s: 2828427124.74619f, v: 8000000000000000000f }]"

[[test.results]]
value = "8000000000000000000f"

[[test.results]]
value = "2828427124.74619f"



















































*/

DEFINE TABLE u SCHEMALESS;
CREATE u:1 SET g = "a", f = 100000001f;
CREATE u:2 SET g = "a", f = 100000002f;
CREATE u:3 SET g = "a", f = 100000004f;

-- Ad-hoc GROUP BY, and the scalar form for reference. Both engines agree.
SELECT g, math::variance(f) AS v, math::stddev(f) AS s FROM u GROUP BY g;
RETURN math::variance([100000001f, 100000002f, 100000004f]);

-- A materialized view backfilled over the rows above. The backfill derives its
-- state from math::mean and math::variance rather than a raw power-sum, so it
-- starts both accurate and well conditioned.
DEFINE TABLE aggb AS SELECT g, math::variance(f) AS v FROM u GROUP BY g;
SELECT VALUE v FROM aggb;

-- A Decimal group stays Decimal, and matches the scalar exactly.
DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET g = "a", d = 1.5dec;
CREATE t:2 SET g = "a", d = 2.5dec;
CREATE t:3 SET g = "a", d = 4.5dec;
SELECT g, math::variance(d) AS v FROM t GROUP BY g;
RETURN math::variance([1.5dec, 2.5dec, 4.5dec]);

-- A materialized view built incrementally, then maintained through a delete and
-- an update. Both stay O(1) -- the accumulators are still invertible.
DEFINE TABLE w SCHEMALESS;
DEFINE TABLE aggi AS SELECT g, math::variance(f) AS v FROM w GROUP BY g;
CREATE w:1 SET g = "a", f = 100000001f;
CREATE w:2 SET g = "a", f = 100000002f;
CREATE w:3 SET g = "a", f = 100000004f;
SELECT VALUE v FROM aggi;

DELETE w:3;
SELECT VALUE v FROM aggi;
RETURN math::variance([100000001f, 100000002f]);

UPDATE w:2 SET f = 100000009f;
SELECT VALUE v FROM aggi;
RETURN math::variance([100000001f, 100000009f]);

-- Integer inputs whose difference exceeds sqrt(i64::MAX) (~3.04e9). Squaring the
-- difference as an i64 would overflow and fail the statement, so it widens to
-- f64 first -- which is what the scalar form does via its Float mean.
DEFINE TABLE i SCHEMALESS;
CREATE i:1 SET g = "a", n = 0;
CREATE i:2 SET g = "a", n = 4000000000;
SELECT g, math::variance(n) AS v, math::stddev(n) AS s FROM i GROUP BY g;
RETURN math::variance([0, 4000000000]);
RETURN math::stddev([0, 4000000000]);
