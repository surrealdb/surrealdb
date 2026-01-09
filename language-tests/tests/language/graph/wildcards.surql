/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test wildcard edge traversal (?)"

[[test.results]]
value = "[has_skill:alice_go, has_skill:alice_pg, has_skill:alice_redis, has_skill:alice_rust, knows:alice_bob, knows:alice_ceo, mentors:alice_bob, reports_to:alice_to_li, works_on:alice_auth, works_on:alice_db]"

[[test.results]]
value = "[skill:go, skill:postgresql, skill:redis, skill:rust, person:bob, person:ceo, person:bob, person:lead_infra, project:auth, project:database]"

[[test.results]]
value = "[knows:bob_alice, knows:charlie_alice, mentors:li_alice]"

[[test.results]]
value = "[person:bob, person:charlie, person:lead_infra]"

[[test.results]]
value = "[knows:bob_alice, knows:charlie_alice, mentors:li_alice, has_skill:alice_go, has_skill:alice_pg, has_skill:alice_redis, has_skill:alice_rust, knows:alice_bob, knows:alice_ceo, mentors:alice_bob, reports_to:alice_to_li, works_on:alice_auth, works_on:alice_db]"

[[test.results]]
value = "[depends_on:api_db, depends_on:auth_db, works_on:alice_db, works_on:bob_db, works_on:li_db]"

[[test.results]]
value = "[depends_on:fe_api, depends_on:fe_auth]"

[[test.results]]
value = "[{ id: person:alice, incoming: [person:bob, person:charlie, person:lead_infra], name: 'Alice Zhang', outgoing: [skill:go, skill:postgresql, skill:redis, skill:rust, person:bob, person:ceo, person:bob, person:lead_infra, project:auth, project:database] }]"
*/

-- All outgoing edges from alice (any edge type)
person:alice->?;

-- All outgoing edges with destination
person:alice->?->?;

-- All incoming edges to alice
person:alice<-?;

-- Incoming edges with source
person:alice<-?<-?;

-- All edges in both directions from alice
person:alice<->?;

-- All incoming edges to a project
project:database<-?;

-- All edges from frontend project
project:frontend->?;

-- Find all connected entities from alice
SELECT 
    id,
    name,
    ->?->?.id AS outgoing,
    <-?<-?.id AS incoming
FROM person:alice;

