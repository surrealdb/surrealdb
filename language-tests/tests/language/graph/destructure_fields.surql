/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test field selection via destructuring in graph queries"

[[test.results]]
value = "[{ id: person:bob, name: 'Bob Martinez', title: 'Infrastructure Engineer' }, { id: person:ceo, name: 'Elena Rodriguez', title: 'CEO' }]"

[[test.results]]
value = "[{ id: project:auth, name: 'Authentication Service', status: 'active' }, { id: project:database, name: 'Core Database', status: 'active' }]"

[[test.results]]
value = "[{ category: 'backend', id: skill:go, name: 'Go' }, { category: 'database', id: skill:postgresql, name: 'PostgreSQL' }, { category: 'database', id: skill:redis, name: 'Redis' }, { category: 'backend', id: skill:rust, name: 'Rust' }]"

[[test.results]]
value = "[{ level: 4, name: 'Priya Patel', title: 'Infrastructure Lead' }]"

[[test.results]]
value = "[{ friends: [{ name: 'Bob Martinez', title: 'Infrastructure Engineer' }, { name: 'Elena Rodriguez', title: 'CEO' }], id: person:alice, name: 'Alice Zhang' }]"

[[test.results]]
value = "[{ id: person:alice, name: 'Alice Zhang', projects: [{ name: 'Authentication Service', status: 'active' }, { name: 'Core Database', status: 'active' }], skills: [{ category: 'backend', name: 'Go' }, { category: 'database', name: 'PostgreSQL' }, { category: 'database', name: 'Redis' }, { category: 'backend', name: 'Rust' }] }]"
*/

-- Select specific fields from targets
person:alice->knows->person.{id, name, title};

-- Select fields from project targets
person:alice->works_on->project.{id, name, status};

-- Select fields from skill targets
person:alice->has_skill->skill.{id, name, category};

-- Multiple fields from reports_to targets
person:alice->reports_to->person.{name, title, level};

-- Select with destructured traversal in SELECT
SELECT 
    id,
    name,
    ->knows->person.{name, title} AS friends
FROM person:alice;

-- Multiple destructured traversals
SELECT 
    id,
    name,
    ->works_on->project.{name, status} AS projects,
    ->has_skill->skill.{name, category} AS skills
FROM person:alice;

