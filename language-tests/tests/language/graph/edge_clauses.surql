/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test ORDER BY, GROUP BY, LIMIT, START on edges"

# 0
[[test.results]]
value = "{ edges: [knows:alice_bob, knows:alice_ceo] }"

# 1
[[test.results]]
value = "{ edges: [{ strength: 'strong' }, { strength: 'weak' }] }"

# 2
[[test.results]]
value = "{ edges: [{ strength: 'strong' }] }"

# 3
[[test.results]]
value = "{ edges: [{ since: d'2020-03-20T00:00:00Z', strength: 'strong' }, { since: d'2022-12-15T00:00:00Z', strength: 'weak' }] }"

# 4
[[test.results]]
value = "{ edges: [{ count: 1, strength: 'strong' }, { count: 1, strength: 'weak' }] }"

# 5
[[test.results]]
value = "{ edges: [knows:alice_bob] }"

# 6
[[test.results]]
value = "{ edges: [knows:alice_ceo] }"

# 7
[[test.results]]
value = "{ edges: [{ id: knows:alice_bob, in: person:alice, out: person:bob, since: d'2020-03-20T00:00:00Z', strength: 'strong' }, { id: knows:alice_ceo, in: person:alice, out: person:ceo, since: d'2022-12-15T00:00:00Z', strength: 'weak' }] }"
*/

-- 0: Basic edge selection
SELECT ->knows AS edges FROM ONLY person:alice;

-- 1: Subquery selecting specific fields from edges
SELECT ->(SELECT strength FROM knows) AS edges FROM ONLY person:alice;

-- 2: Subquery with filtering
SELECT ->(SELECT strength FROM knows WHERE strength = "strong") AS edges FROM ONLY person:alice;

-- 3: ORDER BY on edge fields (ascending by strength)
SELECT ->(SELECT since, strength FROM knows ORDER BY strength ASC) AS edges FROM ONLY person:alice;

-- 4: GROUP BY on edge field (strength)
SELECT ->(SELECT count() AS count, strength FROM knows GROUP BY strength ORDER BY strength) AS edges FROM ONLY person:alice;

-- 5: LIMIT on edges
SELECT ->(knows LIMIT 1) AS edges FROM ONLY person:alice;

-- 6: START (skip first) on edges
SELECT ->(knows START 1) AS edges FROM ONLY person:alice;

-- 7: Order, start, and limit together
SELECT ->(SELECT * FROM knows ORDER BY since ASC START 0 LIMIT 2) AS edges FROM ONLY person:alice;
