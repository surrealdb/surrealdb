/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test combined edge and target node filtering"

[[test.results]]
value = "[skill:rust]"

[[test.results]]
value = "[skill:postgresql]"

[[test.results]]
value = "[project:database]"

[[test.results]]
value = "[project:database]"

[[test.results]]
value = "[{ id: person:alice, name: 'Alice Zhang', strong_friends: ['Bob Martinez'] }]"

[[test.results]]
value = "[{ expert_skills: ['PostgreSQL', 'Rust'], id: person:alice, name: 'Alice Zhang' }, { expert_skills: ['React', 'TypeScript'], id: person:charlie, name: 'Charlie Brown' }]"
*/

-- Expert level at backend skills
person:alice->(has_skill WHERE level = "expert")->(skill WHERE category = "backend");

-- Expert level database skills
person:alice->(has_skill WHERE level = "expert")->(skill WHERE category = "database");

-- Lead role on active high-priority projects
person:alice->(works_on WHERE role = "lead")->(project WHERE status = "active" AND priority <= 2);

-- Significant hours (>15) on active projects
person:alice->(works_on WHERE hours > 15)->(project WHERE status = "active");

-- Get alice's strong connections with details
SELECT 
    id, 
    name,
    ->(knows WHERE strength = "strong")->person.name AS strong_friends
FROM person:alice;

-- Get expert skills for multiple people
SELECT 
    id,
    name,
    ->(has_skill WHERE level = "expert")->skill.name AS expert_skills
FROM person:alice, person:charlie;

