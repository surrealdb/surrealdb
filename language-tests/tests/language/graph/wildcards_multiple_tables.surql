/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test multiple specific edge tables in graph traversal"

# 0
[[test.results]]
value = "[person:dir_platform, person:alice]"

# 1
[[test.results]]
value = "[person:bob, person:ceo, person:bob]"

# 2
[[test.results]]
value = "[person:lead_infra, person:alice, person:charlie, person:dana, skill:postgresql, skill:python, skill:rust]"

# 3
[[test.results]]
value = "[{ all_edges: [reports_to:alice_to_li, knows:alice_bob, knows:alice_ceo, works_on:alice_auth, works_on:alice_db, has_skill:alice_go, has_skill:alice_pg, has_skill:alice_redis, has_skill:alice_rust, mentors:alice_bob], id: person:alice }]"

# 4
[[test.results]]
value = "[{ connections: ['Priya Patel', 'Bob Martinez', 'Elena Rodriguez', 'Bob Martinez'], id: person:alice, name: 'Alice Zhang' }]"
*/

-- 0: Either reports_to or mentors edges from lead_infra
person:lead_infra->(reports_to, mentors)->person;

-- 1: Either knows or mentors edges from alice
person:alice->(knows, mentors)->person;

-- 2: Multiple edge types from bob
person:bob->(reports_to, knows, has_skill)->?;

-- 3: Get all relationship types for alice
SELECT 
    id,
    ->(reports_to, knows, works_on, has_skill, mentors) AS all_edges
FROM person:alice;

-- 4: Get person connections via multiple edge types
SELECT 
    id,
    name,
    ->(reports_to, knows, mentors)->person.name AS connections
FROM person:alice;

