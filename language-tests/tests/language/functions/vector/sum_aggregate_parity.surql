/**
[test]
reason = "Covers the vector::sum aggregate cases whose result does not depend on the planner strategy, so it runs unpinned and guards the compute-only path's Accumulate fallback as well as the streaming aggregate operator. Every group here holds a vector of a consistent dimension on every row: the strategies diverge only on missing vectors, mixed dimensions, per-row functions inside the aggregate argument, and GROUP ALL over an empty table, which sum_aggregate.surql covers under a streaming-only pin."

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ total: [1, 1, 0], user: 'alice' }, { total: [0, 0, 4], user: 'bob' }]"

[[test.results]]
value = "[{ total: [1, 1, 4] }]"

[[test.results]]
value = "[{ total: [0, 0, 4], user: 'bob' }]"
*/
CREATE engagement:1 SET user = 'alice', embedding = [1, 0, 0] RETURN NONE;
CREATE engagement:2 SET user = 'alice', embedding = [0, 1, 0] RETURN NONE;
CREATE engagement:3 SET user = 'bob', embedding = [0, 0, 4] RETURN NONE;

-- Elementwise total per group.
SELECT user, vector::sum(embedding) AS total FROM engagement GROUP BY user ORDER BY user;

-- Elementwise total over every row at once.
SELECT vector::sum(embedding) AS total FROM engagement GROUP ALL;

-- A single-row group returns that row's vector unchanged.
SELECT user, vector::sum(embedding) AS total FROM engagement WHERE user = 'bob' GROUP BY user;
