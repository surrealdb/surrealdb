/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test chained multi-hop graph traversal"

[[test.results]]
value = "[person:dir_platform]"

[[test.results]]
value = "[person:vp_eng]"

[[test.results]]
value = "[person:ceo]"

[[test.results]]
value = "[person:alice, person:bob, person:alice, person:bob, person:lead_infra]"

[[test.results]]
value = "[person:alice, person:charlie, person:dana]"

[[test.results]]
value = "[{ id: person:lead_infra, name: 'Priya Patel', reports_from: ['Alice Zhang', 'Bob Martinez'], reports_to: ['Sarah Kim'] }]"
*/

-- Two hops up the management chain
person:alice->reports_to->person->reports_to->person;

-- Three hops
person:alice->reports_to->person->reports_to->person->reports_to->person;

-- Four hops to CEO
person:alice->reports_to->person->reports_to->person->reports_to->person->reports_to->person;

-- Mixed directions: who works on the same projects as alice?
person:alice->works_on->project<-works_on<-person;

-- Two-hop knows: friends of friends
person:alice->knows->person->knows->person;

-- Both directions in same SELECT
SELECT 
    id, 
    name,
    ->reports_to->person.name AS reports_to,
    <-reports_to<-person.name AS reports_from
FROM person:lead_infra;

