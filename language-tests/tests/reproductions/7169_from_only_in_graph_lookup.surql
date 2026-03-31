/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "FROM ONLY should be supported in graph lookup subqueries"
issue = 7169

# 0: FROM ONLY with LIMIT 1 returns a single unwrapped object
[[test.results]]
value = "{ edge: { id: knows:alice_bob, in: person:alice, out: person:bob, since: d'2020-03-20T00:00:00Z', strength: 'strong' } }"

# 1: FROM ONLY without limit on multiple edges should error
[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

# 2: FROM ONLY with LIMIT 1 and specific field projection
[[test.results]]
value = "{ edge: { strength: 'strong' } }"
*/

-- 0: FROM ONLY with LIMIT 1 inside graph lookup (previously a syntax error)
SELECT ->(SELECT * FROM ONLY knows LIMIT 1) AS edge FROM ONLY person:alice;

-- 1: FROM ONLY without limit on multiple edges should error
SELECT ->(SELECT * FROM ONLY knows) AS edges FROM ONLY person:alice;

-- 2: FROM ONLY with LIMIT 1 and specific field projection
SELECT ->(SELECT strength FROM ONLY knows WHERE strength = "strong" LIMIT 1) AS edge FROM ONLY person:alice;
