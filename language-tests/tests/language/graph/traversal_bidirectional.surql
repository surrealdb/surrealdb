/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test bidirectional edge traversal (<->) patterns"

# 0
[[test.results]]
value = "[person:bob, person:alice, person:charlie, person:alice, person:alice, person:bob, person:alice, person:ceo]"

# 1
[[test.results]]
value = "[person:lead_frontend, person:lead_infra, person:lead_infra, person:lead_frontend]"

# 2
[[test.results]]
value = "[person:dir_product, person:dir_platform, person:dir_platform, person:dir_product]"

# 3
[[test.results]]
value = "[{ connections: [person:bob, person:charlie, person:bob, person:ceo] }]"

# 4
[[test.results]]
value = "[{ connections: ['Bob Martinez', 'Alice Zhang', 'Charlie Brown', 'Alice Zhang', 'Alice Zhang', 'Bob Martinez', 'Alice Zhang', 'Elena Rodriguez'], name: 'Alice Zhang' }]"
*/

-- 0: All connections regardless of direction
person:alice<->knows<->person;

-- 1: Bidirectional from leadership
person:lead_infra<->knows<->person;

-- 2: Directors have bidirectional knows relationships
person:dir_platform<->knows<->person;

-- 3: Filter out self from bidirectional results
SELECT array::complement(<->knows<->person, [id]) AS connections FROM person:alice;

-- 4: Bidirectional with @ syntax
SELECT name, @<->knows<->person.name AS connections FROM person:alice;

