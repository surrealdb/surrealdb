/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = "Tests vector::sum as an aggregate over GROUP BY, including the weighted-centroid composition, GROUP ALL batching, skipped missing vectors, and groups whose vector dimensions disagree. Pinned to the streaming strategies: the compute-only path implements a grouped aggregate by collecting the group's values and applying the scalar form, so it neither skips missing vectors nor evaluates a per-row function inside the aggregate argument. math::sum and math::mean diverge between the two paths the same way."

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ total: [1, 1, 0], user: 'alice' }, { total: [0, 0, 4], user: 'bob' }]"

[[test.results]]
value = "[{ interest: [0.4f, 0.6000000000000001f, 0f], user: 'alice' }, { interest: [0f, 0f, 4f], user: 'bob' }]"

[[test.results]]
value = "[{ interest: [0, 0, 0], user: 'alice' }, { interest: [0, 0, 4], user: 'bob' }]"

[[test.results]]
value = "[{ total: [1, 1, 4] }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ interest: [1f, 3f, 0f], user: 'frank' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ total: [1, 1, 1], user: 'carol' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ total: NONE, user: 'dave' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ total: NULL, user: 'erin' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ total: NONE }]"
*/
CREATE engagement:1 SET user = 'alice', weight = 2, embedding = [1, 0, 0] RETURN NONE;
CREATE engagement:2 SET user = 'alice', weight = 3, embedding = [0, 1, 0] RETURN NONE;
CREATE engagement:3 SET user = 'bob', weight = 1, embedding = [0, 0, 4] RETURN NONE;

-- Elementwise total per user.
SELECT user, vector::sum(embedding) AS total FROM engagement GROUP BY user ORDER BY user;

-- Weighted centroid: the weighted total scaled by one over the total weight.
SELECT user, vector::scale(vector::sum(vector::scale(embedding, weight)), 1.0 / math::sum(weight)) AS interest FROM engagement GROUP BY user ORDER BY user;

-- Integer weights sum to an integer, so an integer reciprocal truncates to zero
-- and scales the weighted total away. The reciprocal has to be a float.
SELECT user, vector::scale(vector::sum(vector::scale(embedding, weight)), 1 / math::sum(weight)) AS interest FROM engagement GROUP BY user ORDER BY user;

-- GROUP ALL folds a whole batch through a single accumulator.
SELECT vector::sum(embedding) AS total FROM engagement GROUP ALL;

-- Float weights sum to a float, which promotes the reciprocal on its own.
CREATE engagement:4 SET user = 'frank', weight = 0.25, embedding = [4, 0, 0] RETURN NONE;
CREATE engagement:5 SET user = 'frank', weight = 0.75, embedding = [0, 4, 0] RETURN NONE;
SELECT user, vector::scale(vector::sum(vector::scale(embedding, weight)), 1 / math::sum(weight)) AS interest FROM engagement WHERE user = 'frank' GROUP BY user;

-- Rows without a vector contribute nothing to their group's total.
CREATE engagement:6 SET user = 'carol', weight = 1 RETURN NONE;
CREATE engagement:7 SET user = 'carol', weight = 1, embedding = [1, 1, 1] RETURN NONE;
SELECT user, vector::sum(embedding) AS total FROM engagement WHERE user = 'carol' GROUP BY user;

-- A group holding no vector at all has no dimension to total.
CREATE engagement:8 SET user = 'dave', weight = 1 RETURN NONE;
SELECT user, vector::sum(embedding) AS total FROM engagement WHERE user = 'dave' GROUP BY user;

-- Disagreeing dimensions leave the group's total undefined rather than summing
-- the rows that happen to agree.
CREATE engagement:9 SET user = 'erin', weight = 1, embedding = [1, 1, 1] RETURN NONE;
CREATE engagement:10 SET user = 'erin', weight = 1, embedding = [1, 1] RETURN NONE;
SELECT user, vector::sum(embedding) AS total FROM engagement WHERE user = 'erin' GROUP BY user;

-- GROUP ALL over a table with no rows still emits the default aggregate row,
-- finalizing a fresh accumulator that has seen no dimension to total. Whether
-- that row is emitted at all is strategy-dependent for every aggregate except
-- count(), which is why this case sits behind the pin.
DEFINE TABLE quiet SCHEMALESS;
SELECT vector::sum(embedding) AS total FROM quiet GROUP ALL;
