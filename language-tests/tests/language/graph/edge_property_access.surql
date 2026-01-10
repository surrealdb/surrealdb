/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test accessing edge properties during traversal"

# 0
[[test.results]]
value = "[knows:alice_bob, knows:alice_ceo]"

# 1
[[test.results]]
value = "[d'2020-03-20T00:00:00Z', d'2022-12-15T00:00:00Z']"

# 2
[[test.results]]
value = "['strong', 'weak']"

# 3
[[test.results]]
value = "[{ hours: 10, role: 'contributor' }, { hours: 30, role: 'lead' }]"

# 4
[[test.results]]
value = "[{ connection_dates: [d'2020-03-20T00:00:00Z', d'2022-12-15T00:00:00Z'], friend_names: ['Bob Martinez', 'Elena Rodriguez'] }]"

# 5
[[test.results]]
value = "[{ assignments: [{ hours: 10, role: 'contributor' }, { hours: 30, role: 'lead' }], projects: [{ name: 'Authentication Service', status: 'active' }, { name: 'Core Database', status: 'active' }] }]"
*/

-- 0: Get edge records (not just target nodes)
person:alice->knows;

-- 1: Get specific edge fields during traversal
person:alice->knows.since;

-- 2: Get specific edge fields during traversal
person:alice->knows.strength;

-- 3: Multiple edge fields via destructuring
person:alice->works_on.{role, hours};

-- 4: Edge dates with friend names
SELECT 
    ->knows.since AS connection_dates,
    ->knows->person.name AS friend_names
FROM person:alice;

-- 5: Work assignments with project details
SELECT 
    ->works_on.{role, hours} AS assignments,
    ->works_on->project.{name, status} AS projects
FROM person:alice;

