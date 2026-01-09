/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test +inclusive modifier to include starting node in results"

[[test.results]]
value = "[person:alice, person:lead_infra, person:dir_platform, person:vp_eng, person:ceo]"

[[test.results]]
value = "['Alice Zhang', 'Priya Patel', 'Sarah Kim', 'Marcus Chen', 'Elena Rodriguez']"

[[test.results]]
value = "[[person:alice, person:lead_infra, person:dir_platform, person:vp_eng, person:ceo]]"

[[test.results]]
value = "[person:alice, person:lead_infra, person:dir_platform, person:vp_eng, person:ceo]"

[[test.results]]
value = "[person:alice, person:lead_infra, person:dir_platform, person:vp_eng]"

[[test.results]]
value = "[person:alice, person:bob]"
*/

-- Include alice in the collect results
person:alice.{..+collect+inclusive}->reports_to->person;

-- Get names including starting node
person:alice.{..+collect+inclusive}(->reports_to->person).name;

-- Include starting node in path results
person:alice.{..+path+inclusive}->reports_to->person;

-- Shortest path including starting node
person:alice.{..+shortest=person:ceo+inclusive}->reports_to->person;

-- VP shortest path with inclusive
person:alice.{..+shortest=person:vp_eng+inclusive}->reports_to->person;

-- Knows shortest path with inclusive
person:alice.{..+shortest=person:bob+inclusive}->knows->person;

