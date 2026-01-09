/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test accessing edge properties during traversal"

[[test.results]]
value = "[knows:alice_bob, knows:alice_ceo]"

[[test.results]]
value = "[d'2020-03-20T00:00:00Z', d'2022-12-15T00:00:00Z']"

[[test.results]]
value = "['strong', 'weak']"

[[test.results]]
value = "[{ hours: 10, role: 'contributor' }, { hours: 30, role: 'lead' }]"

[[test.results]]
value = "[{ connection_dates: [d'2020-03-20T00:00:00Z', d'2022-12-15T00:00:00Z'], friend_names: ['Bob Martinez', 'Elena Rodriguez'] }]"

[[test.results]]
value = "[{ assignments: [{ hours: 10, role: 'contributor' }, { hours: 30, role: 'lead' }], projects: [{ name: 'Authentication Service', status: 'active' }, { name: 'Core Database', status: 'active' }] }]"
*/

-- Get edge records (not just target nodes)
person:alice->knows;

-- Get specific edge fields during traversal
person:alice->knows.since;
person:alice->knows.strength;

-- Multiple edge fields via destructuring
person:alice->works_on.{role, hours};

-- Edge dates with friend names
SELECT 
    ->knows.since AS connection_dates,
    ->knows->person.name AS friend_names
FROM person:alice;

-- Work assignments with project details
SELECT 
    ->works_on.{role, hours} AS assignments,
    ->works_on->project.{name, status} AS projects
FROM person:alice;

