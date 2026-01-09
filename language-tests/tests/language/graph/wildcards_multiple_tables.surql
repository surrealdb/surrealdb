/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test multiple specific edge tables in graph traversal"

[[test.results]]
value = "[person:dir_platform, person:alice]"

[[test.results]]
value = "[person:bob, person:ceo, person:bob]"

[[test.results]]
value = "[person:lead_infra, person:alice, person:charlie, person:dana, skill:postgresql, skill:python, skill:rust]"

[[test.results]]
value = "[{ all_edges: [reports_to:alice_to_li, knows:alice_bob, knows:alice_ceo, works_on:alice_auth, works_on:alice_db, has_skill:alice_go, has_skill:alice_pg, has_skill:alice_redis, has_skill:alice_rust, mentors:alice_bob], id: person:alice }]"

[[test.results]]
value = "[{ connections: ['Priya Patel', 'Bob Martinez', 'Elena Rodriguez', 'Bob Martinez'], id: person:alice, name: 'Alice Zhang' }]"
*/

-- Either reports_to or mentors edges from lead_infra
person:lead_infra->(reports_to, mentors)->person;

-- Either knows or mentors edges from alice
person:alice->(knows, mentors)->person;

-- Multiple edge types from bob
person:bob->(reports_to, knows, has_skill)->?;

-- Get all relationship types for alice
SELECT 
    id,
    ->(reports_to, knows, works_on, has_skill, mentors) AS all_edges
FROM person:alice;

-- Get person connections via multiple edge types
SELECT 
    id,
    name,
    ->(reports_to, knows, mentors)->person.name AS connections
FROM person:alice;

