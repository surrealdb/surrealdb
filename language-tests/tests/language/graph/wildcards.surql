/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test wildcard edge traversal (?)"

# 0
[[test.results]]
value = "[has_skill:alice_go, has_skill:alice_pg, has_skill:alice_redis, has_skill:alice_rust, knows:alice_bob, knows:alice_ceo, mentors:alice_bob, reports_to:alice_to_li, works_on:alice_auth, works_on:alice_db]"

# 1
[[test.results]]
value = "[skill:go, skill:postgresql, skill:redis, skill:rust, person:bob, person:ceo, person:bob, person:lead_infra, project:auth, project:database]"

# 2
[[test.results]]
value = "[knows:bob_alice, knows:charlie_alice, mentors:li_alice]"

# 3
[[test.results]]
value = "[person:bob, person:charlie, person:lead_infra]"

# 4
[[test.results]]
value = "[knows:bob_alice, knows:charlie_alice, mentors:li_alice, has_skill:alice_go, has_skill:alice_pg, has_skill:alice_redis, has_skill:alice_rust, knows:alice_bob, knows:alice_ceo, mentors:alice_bob, reports_to:alice_to_li, works_on:alice_auth, works_on:alice_db]"

# 5
[[test.results]]
value = "[depends_on:api_db, depends_on:auth_db, works_on:alice_db, works_on:bob_db, works_on:li_db]"

# 6
[[test.results]]
value = "[depends_on:fe_api, depends_on:fe_auth]"

# 7
[[test.results]]
value = "[{ id: person:alice, incoming: [person:bob, person:charlie, person:lead_infra], name: 'Alice Zhang', outgoing: [skill:go, skill:postgresql, skill:redis, skill:rust, person:bob, person:ceo, person:bob, person:lead_infra, project:auth, project:database] }]"
*/

-- 0: All outgoing edges from alice (any edge type)
person:alice->?;

-- 1: All outgoing edges with destination
person:alice->?->?;

-- 2: All incoming edges to alice
person:alice<-?;

-- 3: Incoming edges with source
person:alice<-?<-?;

-- 4: All edges in both directions from alice
person:alice<->?;

-- 5: All incoming edges to a project
project:database<-?;

-- 6: All edges from frontend project
project:frontend->?;

-- 7: Find all connected entities from alice
SELECT 
    id,
    name,
    ->?->?.id AS outgoing,
    <-?<-?.id AS incoming
FROM person:alice;

