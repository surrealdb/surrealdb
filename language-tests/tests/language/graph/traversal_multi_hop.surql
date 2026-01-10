/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test chained multi-hop graph traversal"

# 0
[[test.results]]
value = "[person:dir_platform]"

# 1
[[test.results]]
value = "[person:vp_eng]"

# 2
[[test.results]]
value = "[person:ceo]"

# 3
[[test.results]]
value = "[person:alice, person:bob, person:alice, person:bob, person:lead_infra]"

# 4
[[test.results]]
value = "[person:alice, person:charlie, person:dana]"

# 5
[[test.results]]
value = "[{ id: person:lead_infra, name: 'Priya Patel', reports_from: ['Alice Zhang', 'Bob Martinez'], reports_to: ['Sarah Kim'] }]"
*/

-- 0: Two hops up the management chain
person:alice->reports_to->person->reports_to->person;

-- 1: Three hops
person:alice->reports_to->person->reports_to->person->reports_to->person;

-- 2: Four hops to CEO
person:alice->reports_to->person->reports_to->person->reports_to->person->reports_to->person;

-- 3: Mixed directions: who works on the same projects as alice?
person:alice->works_on->project<-works_on<-person;

-- 4: Two-hop knows: friends of friends
person:alice->knows->person->knows->person;

-- 5: Both directions in same SELECT
SELECT 
    id, 
    name,
    ->reports_to->person.name AS reports_to,
    <-reports_to<-person.name AS reports_from
FROM person:lead_infra;

