/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test field selection via destructuring in graph queries"

# 0
[[test.results]]
value = "[{ id: person:bob, name: 'Bob Martinez', title: 'Infrastructure Engineer' }, { id: person:ceo, name: 'Elena Rodriguez', title: 'CEO' }]"

# 1
[[test.results]]
value = "[{ id: project:auth, name: 'Authentication Service', status: 'active' }, { id: project:database, name: 'Core Database', status: 'active' }]"

# 2
[[test.results]]
value = "[{ category: 'backend', id: skill:go, name: 'Go' }, { category: 'database', id: skill:postgresql, name: 'PostgreSQL' }, { category: 'database', id: skill:redis, name: 'Redis' }, { category: 'backend', id: skill:rust, name: 'Rust' }]"

# 3
[[test.results]]
value = "[{ level: 4, name: 'Priya Patel', title: 'Infrastructure Lead' }]"

# 4
[[test.results]]
value = "[{ friends: [{ name: 'Bob Martinez', title: 'Infrastructure Engineer' }, { name: 'Elena Rodriguez', title: 'CEO' }], id: person:alice, name: 'Alice Zhang' }]"

# 5
[[test.results]]
value = "[{ id: person:alice, name: 'Alice Zhang', projects: [{ name: 'Authentication Service', status: 'active' }, { name: 'Core Database', status: 'active' }], skills: [{ category: 'backend', name: 'Go' }, { category: 'database', name: 'PostgreSQL' }, { category: 'database', name: 'Redis' }, { category: 'backend', name: 'Rust' }] }]"
*/

-- 0: Select specific fields from targets
person:alice->knows->person.{id, name, title};

-- 1: Select fields from project targets
person:alice->works_on->project.{id, name, status};

-- 2: Select fields from skill targets
person:alice->has_skill->skill.{id, name, category};

-- 3: Multiple fields from reports_to targets
person:alice->reports_to->person.{name, title, level};

-- 4: Select with destructured traversal in SELECT
SELECT 
    id,
    name,
    ->knows->person.{name, title} AS friends
FROM person:alice;

-- 5: Multiple destructured traversals
SELECT 
    id,
    name,
    ->works_on->project.{name, status} AS projects,
    ->has_skill->skill.{name, category} AS skills
FROM person:alice;

