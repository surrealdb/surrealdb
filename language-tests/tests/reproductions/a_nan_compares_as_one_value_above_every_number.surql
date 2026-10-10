/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
Every NaN, whatever its sign, is one value that sorts above every other number,
infinity included: the place an index key gives every NaN. So no NaN is below
zero, every NaN is above it, two NaNs are equal, and an indexed read answers
what its WITH NOINDEX twin answers. The table stores `0.0 / 0.0` and its
negation, so a NaN carrying the sign bit is present whichever sign the platform
gives the quotient."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:nan, v: NaN }]"

[[test.results]]
value = "[{ id: t:negated_nan, v: NaN }]"

[[test.results]]
value = "[{ id: t:minus_one, v: -1 }]"

[[test.results]]
value = "[{ id: t:one, v: 1 }]"

[[test.results]]
value = "[{ id: t:infinity, v: Infinity }]"

[[test.results]]
value = "[t:minus_one]"

[[test.results]]
value = "[t:minus_one]"

[[test.results]]
value = "[t:infinity, t:nan, t:negated_nan, t:one]"

[[test.results]]
value = "[t:infinity, t:nan, t:negated_nan, t:one]"

[[test.results]]
value = "[t:nan, t:negated_nan]"

[[test.results]]
value = "[t:nan, t:negated_nan]"

[[test.results]]
value = "[t:nan, t:negated_nan]"

[[test.results]]
value = "[t:nan, t:negated_nan]"

[[test.results]]
value = "true"

[[test.results]]
value = "[-1, 1, Infinity, NaN, NaN]"

*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX t_v ON t FIELDS v;
CREATE t:nan SET v = 0.0 / 0.0;
CREATE t:negated_nan SET v = -(0.0 / 0.0);
CREATE t:minus_one SET v = -1;
CREATE t:one SET v = 1;
CREATE t:infinity SET v = 1.0 / 0.0;

-- Below zero: -1 alone.
array::sort(SELECT VALUE id FROM t WHERE v < 0);
array::sort(SELECT VALUE id FROM t WITH NOINDEX WHERE v < 0);

-- Above zero: 1, infinity and both NaNs.
array::sort(SELECT VALUE id FROM t WHERE v > 0);
array::sort(SELECT VALUE id FROM t WITH NOINDEX WHERE v > 0);

-- Above infinity: both NaNs and nothing else.
array::sort(SELECT VALUE id FROM t WHERE v > 1.0 / 0.0);
array::sort(SELECT VALUE id FROM t WITH NOINDEX WHERE v > 1.0 / 0.0);

-- Equal to a NaN: both NaNs.
array::sort(SELECT VALUE id FROM t WHERE v = 0.0 / 0.0);
array::sort(SELECT VALUE id FROM t WITH NOINDEX WHERE v = 0.0 / 0.0);

-- The two NaNs are one value, and an ORDER BY places them last.
t:nan.v == t:negated_nan.v;
SELECT VALUE v FROM t ORDER BY v;
