/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test aliasing (flattened) vs graph structure preservation"

[[test.results]]
value = "[{ manager: [person:lead_infra], skip_level: [person:dir_platform] }]"

[[test.results]]
value = "[{ contacts: [person:bob, person:ceo], projects: [project:auth, project:database], skills: [skill:go, skill:postgresql, skill:redis, skill:rust] }]"

[[test.results]]
value = "[{ manager_name: ['Priya Patel'], manager_title: ['Infrastructure Lead'], skip_level_name: ['Sarah Kim'] }]"

[[test.results]]
value = """[{ "->reports_to": { "->person": [{ "->reports_to": { "->person": [{ id: person:dir_platform, name: 'Sarah Kim' }] }, id: person:lead_infra, name: 'Priya Patel' }] } }]"""

[[test.results]]
value = "[{ friend_names_upper: ['BOB MARTINEZ', 'ELENA RODRIGUEZ'] }]"

[[test.results]]
value = "[{ first_friend: 'Bob Martinez', friend_count: 2 }]"
*/

-- With aliases - results are flattened
SELECT 
    ->reports_to->person AS manager,
    ->reports_to->person->reports_to->person AS skip_level
FROM person:alice;

-- Multiple aliased traversals
SELECT 
    ->works_on->project AS projects,
    ->has_skill->skill AS skills,
    ->knows->person AS contacts
FROM person:alice;

-- Aliased nested traversal
SELECT 
    ->reports_to->person.name AS manager_name,
    ->reports_to->person.title AS manager_title,
    ->reports_to->person->reports_to->person.name AS skip_level_name
FROM person:alice;

-- Without aliases - preserves graph structure
SELECT 
    ->reports_to->person.{id, name},
    ->reports_to->person->reports_to->person.{id, name}
FROM person:alice;

-- Uppercase names via alias
SELECT 
    array::map(->knows->person.name, |$x| string::uppercase($x)) AS friend_names_upper
FROM person:alice;

-- Array operations on traversal results
SELECT 
    array::len(->knows->person) AS friend_count,
    array::first(->knows->person).name AS first_friend
FROM person:alice;

